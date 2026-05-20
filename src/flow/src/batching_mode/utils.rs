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

//! some utils for helping with batching mode

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_function::aggrs::aggr_wrapper::get_aggr_func;
use common_telemetry::debug;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::Unparser;
use datafusion_common::tree_node::{
    Transformed, TreeNode as _, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{
    Column, DFSchema, DataFusionError, NullEquality, ScalarValue, TableReference,
};
use datafusion_expr::logical_plan::TableScan;
use datafusion_expr::{
    Distinct, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, and, binary_expr,
    bitwise_and, bitwise_or, bitwise_xor, is_null, lit, or, when,
};
use datatypes::schema::{ColumnSchema, SchemaRef};
use query::QueryEngineRef;
use query::parser::{DEFAULT_LOOKBACK_STRING, PromQuery, QueryLanguageParser, QueryStatement};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use sql::statements::tql::Tql;
use table::TableRef;
use table::table::adapter::DfTableProviderAdapter;

use crate::adapter::{AUTO_CREATED_PLACEHOLDER_TS_COL, AUTO_CREATED_UPDATE_AT_TS_COL};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu, TableNotFoundSnafu};
use crate::{Error, TableName};

/// Describes how one aggregate output field should be merged with the
/// corresponding existing field in the sink table.
///
/// `output_field_name` is the final output/sink schema field name produced by
/// the delta plan and read from the sink table. It is not a DataFusion `Column`
/// reference. It may contain dots or other non-identifier characters when the
/// query keeps DataFusion's raw aggregate output name, e.g.
/// `max(numbers_with_ts.number)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAggregateMergeColumn {
    /// Final output/sink field name for the aggregate result/state column.
    ///
    pub output_field_name: String,
    pub merge_op: IncrementalAggregateMergeOp,
}

impl IncrementalAggregateMergeColumn {
    /// Create a new merge column.
    pub fn new(output_field_name: String, merge_op: IncrementalAggregateMergeOp) -> Self {
        Self {
            output_field_name,
            merge_op,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrementalAggregateMergeOp {
    Sum,
    Min,
    Max,
    BoolAnd,
    BoolOr,
    BitAnd,
    BitOr,
    BitXor,
}

/// Analysis result for an incremental aggregate plan.
///
/// `group_key_names` and each merge column's `output_field_name` are final
/// output/sink schema field names used to project both the delta plan and the
/// sink table before the left-join merge. They are not DataFusion logical-plan
/// `Column` references; callers must attach qualifiers structurally instead of
/// formatting qualified names as strings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncrementalAggregateAnalysis {
    /// Final output/sink field names for group keys used as merge join keys.
    pub group_key_names: Vec<String>,
    pub merge_columns: Vec<IncrementalAggregateMergeColumn>,
    /// Literal output fields that can be passed through from the delta plan.
    pub literal_columns: Vec<String>,
    /// Final output field order from the original aggregate plan.
    pub output_field_names: Vec<String>,
    pub unsupported_exprs: Vec<String>,
}

/// Visitor that captures aggregate expressions and counts `Aggregate` nodes in
/// the plan tree.
///
/// Incremental aggregate rewrite only supports plans with exactly one aggregate
/// node. The count lets the analyzer reject nested/sibling aggregate plans
/// instead of accidentally rewriting against whichever aggregate was visited
/// last.
#[derive(Default)]
struct AggregateExprFinder {
    aggr_exprs: Option<Vec<Expr>>,
    aggregate_count: usize,
}

impl TreeNodeVisitor<'_> for AggregateExprFinder {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Aggregate(aggregate) = node {
            self.aggregate_count += 1;
            self.aggr_exprs = Some(aggregate.aggr_expr.clone());
        }
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Recursively find all `Expr::Column` names inside an expression tree.
/// Only recurses into wrappers that are merge-transparent.
/// Non-transparent wrappers (e.g., `ScalarFunction`, `Negative`, `Cast`) are
/// intentionally not recursed into since their merge semantics would be
/// incorrect.
///
/// `Cast`/`TryCast` are intentionally opaque: merging already-casted aggregate
/// outputs is not generally equivalent to casting the final merged aggregate.
fn find_column_names(expr: &Expr, names: &mut Vec<String>) {
    match expr {
        Expr::Column(col) => {
            names.push(col.name.clone());
        }
        Expr::Alias(alias) => find_column_names(&alias.expr, names),
        _ => {}
    }
}

fn unqualified_col(name: impl Into<String>) -> Expr {
    Expr::Column(Column::from_name(name.into()))
}

fn qualified_col(qualifier: &str, name: impl Into<String>) -> Expr {
    Expr::Column(Column::new(Some(qualifier), name.into()))
}

fn qualified_column(qualifier: &str, name: impl Into<String>) -> Column {
    Column::new(Some(qualifier), name.into())
}

fn find_group_key_names(plan: &LogicalPlan) -> Result<Vec<String>, Error> {
    let mut group_finder = FindGroupByFinalName::default();
    plan.visit(&mut group_finder)
        .with_context(|_| DatafusionSnafu {
            context: format!("Failed to inspect group-by columns from logical plan: {plan:?}"),
        })?;

    let mut group_key_names = group_finder
        .get_group_expr_names()
        .unwrap_or_default()
        .into_iter()
        .collect::<Vec<_>>();
    group_key_names.sort();
    Ok(group_key_names)
}

fn find_aggregate_exprs(plan: &LogicalPlan) -> Result<(usize, Option<Vec<Expr>>), Error> {
    let mut aggregate_finder = AggregateExprFinder::default();
    plan.visit(&mut aggregate_finder)
        .with_context(|_| DatafusionSnafu {
            context: format!("Failed to inspect aggregate expressions from logical plan: {plan:?}"),
        })?;
    Ok((
        aggregate_finder.aggregate_count,
        aggregate_finder.aggr_exprs,
    ))
}

fn has_grouping_set(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(aggregate) => aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_))),
        _ => plan.inputs().into_iter().any(has_grouping_set),
    }
}

fn check_inc_aggr_plan_shape(plan: &LogicalPlan) -> Result<(), String> {
    // Supported final shape: optional output Projection directly over one
    // Aggregate. Post-aggregate filters (HAVING), ordering, limits,
    // distinct/window/union/extension nodes are intentionally not accepted.
    let plan = match plan {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        _ => plan,
    };

    match plan {
        LogicalPlan::Aggregate(aggregate) => check_input_plan_shape(aggregate.input.as_ref()),
        LogicalPlan::Filter(_) => Err(
            "unsupported post-aggregate filter (HAVING) in incremental aggregate rewrite"
                .to_string(),
        ),
        _ => Err(
            "unsupported post-aggregate plan shape in incremental aggregate rewrite".to_string(),
        ),
    }
}

fn check_input_plan_shape(plan: &LogicalPlan) -> Result<(), String> {
    match plan {
        // Supported aggregate input shape: optional WHERE filter over a table scan.
        LogicalPlan::TableScan(_) => Ok(()),
        LogicalPlan::Filter(filter)
            if matches!(filter.input.as_ref(), LogicalPlan::TableScan(_)) =>
        {
            Ok(())
        }
        _ => Err(
            "unsupported aggregate input plan shape in incremental aggregate rewrite".to_string(),
        ),
    }
}

#[derive(Debug, Default)]
struct OutputProjectionInfo {
    has_top_level_projection: bool,
    output_aliases: HashMap<String, String>,
    duplicate_aggregate_aliases: BTreeSet<String>,
    literal_columns: HashSet<String>,
    output_field_names: Vec<String>,
}

impl OutputProjectionInfo {
    fn output_field_name_set(&self) -> HashSet<String> {
        self.output_field_names.iter().cloned().collect()
    }

    fn duplicate_output_names(&self) -> Vec<String> {
        let mut seen = HashSet::new();
        let mut duplicates = BTreeSet::new();
        for name in &self.output_field_names {
            if !seen.insert(name.clone()) {
                duplicates.insert(name.clone());
            }
        }
        duplicates.into_iter().collect()
    }
}

fn collect_output_projection_info(plan: &LogicalPlan) -> OutputProjectionInfo {
    let mut projection_info = OutputProjectionInfo {
        has_top_level_projection: matches!(plan, LogicalPlan::Projection(_)),
        output_field_names: plan
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect(),
        ..Default::default()
    };

    let mut output_aliases = HashMap::new();
    if let LogicalPlan::Projection(projection) = plan {
        for expr in &projection.expr {
            match expr {
                Expr::Alias(alias) => {
                    // Alias resolution has three cases:
                    // - 0 Column refs (e.g., literal `42 AS lit`): record literal output
                    // - 1 Column ref: record the mapping (e.g., `sum(x) AS total`)
                    // - >1 Column refs (e.g., `COALESCE(sum(x), sum(y))`):
                    //   skip — ambiguous merge semantics
                    let alias_name = alias.name.clone();
                    let mut col_names = Vec::new();
                    find_column_names(&alias.expr, &mut col_names);
                    match col_names.len() {
                        0 if matches!(alias.expr.as_ref(), Expr::Literal(_, _)) => {
                            projection_info.literal_columns.insert(alias_name);
                        }
                        1 => {
                            if let Some(col_name) = col_names.into_iter().next() {
                                if let Some(existing_alias) = output_aliases.get(&col_name) {
                                    if existing_alias != &alias_name {
                                        projection_info.duplicate_aggregate_aliases.insert(format!(
                                            "same aggregate output {col_name} is used by multiple aliases: {existing_alias}, {alias_name}"
                                        ));
                                    }
                                } else {
                                    output_aliases.insert(col_name, alias_name);
                                }
                            }
                        }
                        _ => {}
                    }

                    // If >1 column references detected (e.g., COALESCE(sum(x), sum(y))),
                    // intentionally skip alias mapping — the merge semantics are ambiguous.
                }
                Expr::Column(col) => {
                    output_aliases
                        .entry(col.name.clone())
                        .or_insert(col.name.clone());
                }
                Expr::Literal(_, _) => {
                    projection_info
                        .literal_columns
                        .insert(expr.qualified_name().1);
                }
                _ => {}
            }
        }
    }

    projection_info.output_aliases = output_aliases;
    projection_info
}

fn merge_op_for_aggregate_expr(aggr_expr: &Expr) -> Result<IncrementalAggregateMergeOp, String> {
    let Some(aggr_func) = get_aggr_func(aggr_expr) else {
        return Err(aggr_expr.to_string());
    };
    if aggr_func.params.distinct {
        return Err(format!("unsupported DISTINCT aggregate: {aggr_expr}"));
    }
    if !aggr_func.params.order_by.is_empty() {
        return Err(format!("unsupported aggregate ORDER BY: {aggr_expr}"));
    }
    if aggr_func.params.null_treatment.is_some() {
        return Err(format!("unsupported aggregate NULL treatment: {aggr_expr}"));
    }

    match aggr_func.func.name().to_ascii_lowercase().as_str() {
        "sum" | "count" => Ok(IncrementalAggregateMergeOp::Sum),
        "min" => Ok(IncrementalAggregateMergeOp::Min),
        "max" => Ok(IncrementalAggregateMergeOp::Max),
        "bool_and" => Ok(IncrementalAggregateMergeOp::BoolAnd),
        "bool_or" => Ok(IncrementalAggregateMergeOp::BoolOr),
        "bit_and" => Ok(IncrementalAggregateMergeOp::BitAnd),
        "bit_or" => Ok(IncrementalAggregateMergeOp::BitOr),
        "bit_xor" => Ok(IncrementalAggregateMergeOp::BitXor),
        _ => Err(aggr_expr.to_string()),
    }
}

fn resolve_aggregate_output_field_name(
    aggr_expr: &Expr,
    projection_info: &OutputProjectionInfo,
    output_field_name_set: &HashSet<String>,
) -> Option<String> {
    // qualified_name() returns (Option<String>, String) where the second
    // element is the unqualified column/alias name. This relies on
    // DataFusion's internal naming convention: aggregate expressions
    // emit a column named after the aggregate itself (e.g. "SUM(x)"),
    // which matches what the projection aliases reference.
    let raw_name = aggr_expr.qualified_name().1;
    if let Some(alias) = projection_info.output_aliases.get(&raw_name) {
        Some(alias.clone())
    } else if !projection_info.has_top_level_projection && output_field_name_set.contains(&raw_name)
    {
        Some(raw_name)
    } else {
        None
    }
}

fn find_uncovered_output_fields(
    projection_info: &OutputProjectionInfo,
    group_key_names: &[String],
    merge_columns: &[IncrementalAggregateMergeColumn],
) -> Vec<String> {
    let group_key_names = group_key_names.iter().cloned().collect::<HashSet<_>>();
    let merge_column_names = merge_columns
        .iter()
        .map(|c| c.output_field_name.clone())
        .collect::<HashSet<_>>();

    projection_info
        .output_field_names
        .iter()
        .filter(|name| {
            !group_key_names.contains(*name)
                && !merge_column_names.contains(*name)
                && !projection_info.literal_columns.contains(*name)
        })
        .cloned()
        .collect()
}

pub fn analyze_incremental_aggregate_plan(
    plan: &LogicalPlan,
) -> Result<Option<IncrementalAggregateAnalysis>, Error> {
    let group_key_names = find_group_key_names(plan)?;
    let (aggregate_count, Some(aggr_exprs)) = find_aggregate_exprs(plan)? else {
        return Ok(None);
    };
    let projection_info = collect_output_projection_info(plan);
    let output_field_name_set = projection_info.output_field_name_set();

    let mut merge_columns = Vec::with_capacity(aggr_exprs.len());
    let mut unsupported_exprs = projection_info
        .duplicate_output_names()
        .into_iter()
        .map(|name| format!("duplicate output field name: {name}"))
        .collect::<Vec<_>>();
    if aggregate_count != 1 {
        unsupported_exprs.push(format!(
            "unsupported aggregate plan contains {aggregate_count} Aggregate nodes"
        ));
    }
    if let Err(reason) = check_inc_aggr_plan_shape(plan) {
        unsupported_exprs.push(reason);
    }
    if has_grouping_set(plan) {
        unsupported_exprs.push(
            "unsupported GROUPING SETS/CUBE/ROLLUP in incremental aggregate rewrite".to_string(),
        );
    }
    if group_key_names.is_empty() {
        unsupported_exprs
            .push("unsupported global aggregate in incremental aggregate rewrite".to_string());
    }
    unsupported_exprs.extend(projection_info.duplicate_aggregate_aliases.iter().cloned());
    for aggr_expr in aggr_exprs {
        let merge_op = match merge_op_for_aggregate_expr(&aggr_expr) {
            Ok(merge_op) => merge_op,
            Err(reason) => {
                unsupported_exprs.push(reason);
                continue;
            }
        };
        let Some(output_field_name) = resolve_aggregate_output_field_name(
            &aggr_expr,
            &projection_info,
            &output_field_name_set,
        ) else {
            unsupported_exprs.push(aggr_expr.to_string());
            continue;
        };
        merge_columns.push(IncrementalAggregateMergeColumn::new(
            output_field_name,
            merge_op,
        ));
    }
    unsupported_exprs.extend(
        find_uncovered_output_fields(&projection_info, &group_key_names, &merge_columns)
            .into_iter()
            .map(|name| format!("unsupported output field: {name}")),
    );
    if !unsupported_exprs.is_empty() {
        merge_columns.clear();
    }
    let mut literal_columns = projection_info
        .literal_columns
        .into_iter()
        .collect::<Vec<_>>();
    literal_columns.sort();

    Ok(Some(IncrementalAggregateAnalysis {
        group_key_names,
        merge_columns,
        literal_columns,
        output_field_names: projection_info.output_field_names,
        unsupported_exprs,
    }))
}

/// Rewrites one incremental aggregate delta plan by left-joining it with the
/// existing sink-table state and projecting merged aggregate outputs.
///
/// For a grouped aggregate such as:
///
/// ```text
/// SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts
/// ```
///
/// the rewrite is roughly:
///
/// ```text
/// delta = SELECT ts, number FROM <delta_plan> AS __flow_delta
/// sink  = SELECT ts, number FROM <sink_table> AS __flow_sink
/// SELECT
///   CASE
///     WHEN __flow_sink.number IS NULL THEN __flow_delta.number
///     WHEN __flow_delta.number >= __flow_sink.number THEN __flow_delta.number
///     ELSE __flow_sink.number
///   END AS number,
///   __flow_delta.ts AS ts
/// FROM delta
/// LEFT JOIN sink
///   ON __flow_delta.ts IS NOT DISTINCT FROM __flow_sink.ts
/// ```
pub async fn rewrite_incremental_aggregate_with_sink_merge(
    delta_plan: &LogicalPlan,
    analysis: &IncrementalAggregateAnalysis,
    sink_table: TableRef,
    sink_table_name: &TableName,
) -> Result<LogicalPlan, Error> {
    ensure!(
        analysis.unsupported_exprs.is_empty(),
        InvalidQuerySnafu {
            reason: format!(
                "UNSUPPORTED_INCREMENTAL_AGG: unsupported aggregate expressions {:?}",
                analysis.unsupported_exprs
            )
        }
    );

    ensure!(
        !analysis.merge_columns.is_empty(),
        InvalidQuerySnafu {
            reason:
                "UNSUPPORTED_INCREMENTAL_AGG: aggregate query has no mergeable aggregate columns"
                    .to_string()
        }
    );

    ensure!(
        !analysis.group_key_names.is_empty(),
        InvalidQuerySnafu {
            reason: "UNSUPPORTED_INCREMENTAL_AGG: global aggregate query is not supported"
                .to_string()
        }
    );

    let delta_alias = "__flow_delta";
    let sink_alias = "__flow_sink";

    let mut selected_columns = analysis.group_key_names.clone();
    selected_columns.extend(
        analysis
            .merge_columns
            .iter()
            .map(|c| c.output_field_name.clone()),
    );
    let mut delta_selected_columns = selected_columns.clone();
    delta_selected_columns.extend(analysis.literal_columns.iter().cloned());

    let delta_selected_exprs = delta_selected_columns
        .iter()
        .cloned()
        .map(unqualified_col)
        .collect::<Vec<_>>();
    let delta_selected = LogicalPlanBuilder::from(delta_plan.clone())
        .project(delta_selected_exprs)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to project delta plan for incremental sink merge".to_string(),
        })?
        .alias(delta_alias)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to alias delta plan for incremental sink merge".to_string(),
        })?
        .build()
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build projected delta plan for incremental sink merge".to_string(),
        })?;

    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let sink_scan = LogicalPlan::TableScan(
        TableScan::try_new(
            TableReference::Full {
                catalog: sink_table_name[0].clone().into(),
                schema: sink_table_name[1].clone().into(),
                table: sink_table_name[2].clone().into(),
            },
            table_source,
            None,
            vec![],
            None,
        )
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build sink table scan for incremental sink merge".to_string(),
        })?,
    );

    let sink_selected_exprs = selected_columns
        .iter()
        .cloned()
        .map(unqualified_col)
        .collect::<Vec<_>>();
    let sink_selected = LogicalPlanBuilder::from(sink_scan)
        .project(sink_selected_exprs)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to project sink table scan for incremental sink merge".to_string(),
        })?
        .alias(sink_alias)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to alias sink plan for incremental sink merge".to_string(),
        })?
        .build()
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build projected sink plan for incremental sink merge".to_string(),
        })?;

    let join_keys = (
        analysis
            .group_key_names
            .iter()
            .cloned()
            .map(|c| qualified_column(delta_alias, c))
            .collect::<Vec<_>>(),
        analysis
            .group_key_names
            .iter()
            .cloned()
            .map(|c| qualified_column(sink_alias, c))
            .collect::<Vec<_>>(),
    );

    let joined = LogicalPlanBuilder::from(delta_selected)
        .join_detailed(
            sink_selected,
            JoinType::Left,
            join_keys,
            None,
            NullEquality::NullEqualsNull,
        )
        .with_context(|_| DatafusionSnafu {
            context: "Failed to left join delta and sink plans for incremental sink merge"
                .to_string(),
        })?
        .build()
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build left join plan for incremental sink merge".to_string(),
        })?;

    let group_key_names = analysis.group_key_names.iter().collect::<HashSet<_>>();
    let literal_columns = analysis.literal_columns.iter().collect::<HashSet<_>>();
    let merge_columns = analysis
        .merge_columns
        .iter()
        .map(|c| (&c.output_field_name, c))
        .collect::<HashMap<_, _>>();

    let mut projection_exprs = Vec::with_capacity(analysis.output_field_names.len());
    for output_field_name in &analysis.output_field_names {
        if group_key_names.contains(output_field_name)
            || literal_columns.contains(output_field_name)
        {
            projection_exprs.push(
                qualified_col(delta_alias, output_field_name.clone()).alias(output_field_name),
            );
        } else if let Some(merge_col) = merge_columns.get(output_field_name) {
            projection_exprs.push(build_left_join_merge_expr(
                delta_alias,
                sink_alias,
                merge_col,
            )?);
        } else {
            return InvalidQuerySnafu {
                reason: format!(
                    "UNSUPPORTED_INCREMENTAL_AGG: output field {output_field_name} is not covered by group keys, literals, or merge columns"
                ),
            }
            .fail();
        }
    }

    LogicalPlanBuilder::from(joined)
        .project(projection_exprs)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to build projection merge plan for incremental sink merge".to_string(),
        })?
        .build()
        .with_context(|_| DatafusionSnafu {
            context: "Failed to finalize incremental aggregate sink merge plan".to_string(),
        })
}

fn build_left_join_merge_expr(
    delta_alias: &str,
    sink_alias: &str,
    merge_col: &IncrementalAggregateMergeColumn,
) -> Result<Expr, Error> {
    let left = qualified_col(delta_alias, merge_col.output_field_name.clone());
    let right = qualified_col(sink_alias, merge_col.output_field_name.clone());
    let merged = match merge_col.merge_op {
        IncrementalAggregateMergeOp::Sum => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(binary_expr(left.clone(), Operator::Plus, right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build SUM merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::Min => when(is_null(right.clone()), left.clone())
            .when(left.clone().lt_eq(right.clone()), left.clone())
            .otherwise(right.clone())
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build MIN merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::Max => when(is_null(right.clone()), left.clone())
            .when(left.clone().gt_eq(right.clone()), left.clone())
            .otherwise(right.clone())
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build MAX merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::BoolAnd => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(and(left.clone(), right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build BOOL_AND merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::BoolOr => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(or(left.clone(), right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build BOOL_OR merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::BitAnd => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(bitwise_and(left.clone(), right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build BIT_AND merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::BitOr => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(bitwise_or(left.clone(), right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build BIT_OR merge expression".to_string(),
            })?,
        IncrementalAggregateMergeOp::BitXor => when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(bitwise_xor(left.clone(), right.clone()))
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build BIT_XOR merge expression".to_string(),
            })?,
    };
    Ok(merged.alias(merge_col.output_field_name.clone()))
}

pub async fn get_table_info_df_schema(
    catalog_mr: CatalogManagerRef,
    table_name: TableName,
) -> Result<(TableRef, Arc<DFSchema>), Error> {
    let full_table_name = table_name.clone().join(".");
    let table = catalog_mr
        .table(&table_name[0], &table_name[1], &table_name[2], None)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?
        .context(TableNotFoundSnafu {
            name: &full_table_name,
        })?;
    let table_info = table.table_info();

    let schema = table_info.meta.schema.clone();

    let df_schema: Arc<DFSchema> = Arc::new(
        schema
            .arrow_schema()
            .clone()
            .try_into()
            .with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to convert arrow schema to datafusion schema, arrow_schema={:?}",
                    schema.arrow_schema()
                ),
            })?,
    );
    Ok((table, df_schema))
}

/// Convert sql to datafusion logical plan
/// Also support TQL (but only Eval not Explain or Analyze)
pub async fn sql_to_df_plan(
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sql: &str,
    optimize: bool,
) -> Result<LogicalPlan, Error> {
    let stmts =
        ParserContext::create_with_dialect(sql, query_ctx.sql_dialect(), ParseOptions::default())
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;

    ensure!(
        stmts.len() == 1,
        InvalidQuerySnafu {
            reason: format!("Expect only one statement, found {}", stmts.len())
        }
    );
    let stmt = &stmts[0];
    let query_stmt = match stmt {
        Statement::Tql(tql) => match tql {
            Tql::Eval(eval) => {
                let eval = eval.clone();
                let promql = PromQuery {
                    start: eval.start,
                    end: eval.end,
                    step: eval.step,
                    query: eval.query,
                    lookback: eval
                        .lookback
                        .unwrap_or_else(|| DEFAULT_LOOKBACK_STRING.to_string()),
                    alias: eval.alias.clone(),
                };

                QueryLanguageParser::parse_promql(&promql, &query_ctx)
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?
            }
            _ => InvalidQuerySnafu {
                reason: format!("TQL statement {tql:?} is not supported, expect only TQL EVAL"),
            }
            .fail()?,
        },
        _ => QueryStatement::Sql(stmt.clone()),
    };
    let plan = engine
        .planner()
        .plan(&query_stmt, query_ctx.clone())
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let plan = if optimize {
        apply_df_optimizer(plan, &query_ctx).await?
    } else {
        plan
    };
    Ok(plan)
}

/// Generate a plan that matches the schema of the sink table
/// from given sql by alias and adding auto columns
pub(crate) async fn gen_plan_with_matching_schema(
    sql: &str,
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sink_table_schema: SchemaRef,
    primary_key_indices: &[usize],
    allow_partial: bool,
) -> Result<LogicalPlan, Error> {
    let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), sql, false).await?;

    let mut add_auto_column = ColumnMatcherRewriter::new(
        sink_table_schema,
        primary_key_indices.to_vec(),
        allow_partial,
    );
    let plan = plan
        .clone()
        .rewrite(&mut add_auto_column)
        .with_context(|_| DatafusionSnafu {
            context: format!("Failed to rewrite plan:\n {}\n", plan),
        })?
        .data;
    Ok(plan)
}

pub fn df_plan_to_sql(plan: &LogicalPlan) -> Result<String, Error> {
    /// A dialect that forces identifiers to be quoted when have uppercase
    struct ForceQuoteIdentifiers;
    impl datafusion::sql::unparser::dialect::Dialect for ForceQuoteIdentifiers {
        fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
            if identifier.to_lowercase() != identifier {
                Some('`')
            } else {
                None
            }
        }
    }
    let unparser = Unparser::new(&ForceQuoteIdentifiers);
    // first make all column qualified
    let sql = unparser
        .plan_to_sql(plan)
        .with_context(|_e| DatafusionSnafu {
            context: format!("Failed to unparse logical plan {plan:?}"),
        })?;
    Ok(sql.to_string())
}

/// Helper to find the innermost group by expr in schema, return None if no group by expr
#[derive(Debug, Clone, Default)]
pub struct FindGroupByFinalName {
    group_exprs: Option<HashSet<datafusion_expr::Expr>>,
}

impl FindGroupByFinalName {
    pub fn get_group_expr_names(&self) -> Option<HashSet<String>> {
        self.group_exprs
            .as_ref()
            .map(|exprs| exprs.iter().map(|expr| expr.qualified_name().1).collect())
    }
}

impl TreeNodeVisitor<'_> for FindGroupByFinalName {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Aggregate(aggregate) = node {
            self.group_exprs = Some(aggregate.group_expr.iter().cloned().collect());
            debug!(
                "FindGroupByFinalName: Get Group by exprs from Aggregate: {:?}",
                self.group_exprs
            );
        } else if let LogicalPlan::Distinct(distinct) = node {
            debug!("FindGroupByFinalName: Distinct: {}", node);
            match distinct {
                Distinct::All(input) => {
                    if let LogicalPlan::TableScan(table_scan) = &**input {
                        // get column from field_qualifier, projection and projected_schema:
                        let len = table_scan.projected_schema.fields().len();
                        let columns = (0..len)
                            .map(|f| {
                                let (qualifier, field) =
                                    table_scan.projected_schema.qualified_field(f);
                                datafusion_common::Column::new(qualifier.cloned(), field.name())
                            })
                            .map(datafusion_expr::Expr::Column);
                        self.group_exprs = Some(columns.collect());
                    } else {
                        self.group_exprs = Some(input.expressions().iter().cloned().collect())
                    }
                }
                Distinct::On(distinct_on) => {
                    self.group_exprs = Some(distinct_on.on_expr.iter().cloned().collect())
                }
            }
            debug!(
                "FindGroupByFinalName: Get Group by exprs from Distinct: {:?}",
                self.group_exprs
            );
        }

        Ok(TreeNodeRecursion::Continue)
    }

    /// deal with projection when going up with group exprs
    fn f_up(&mut self, node: &Self::Node) -> datafusion_common::Result<TreeNodeRecursion> {
        if let LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                let Some(group_exprs) = &mut self.group_exprs else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                if let datafusion_expr::Expr::Alias(alias) = expr {
                    // if a alias exist, replace with the new alias
                    let mut new_group_exprs = group_exprs.clone();
                    for group_expr in group_exprs.iter() {
                        if group_expr.name_for_alias()? == alias.expr.name_for_alias()? {
                            new_group_exprs.remove(group_expr);
                            new_group_exprs.insert(expr.clone());
                            break;
                        }
                    }
                    *group_exprs = new_group_exprs;
                }
            }
        }
        debug!("Aliased group by exprs: {:?}", self.group_exprs);
        Ok(TreeNodeRecursion::Continue)
    }
}

/// Optionally add to the final select columns like `update_at` if the sink table has such column
/// (which doesn't necessary need to have exact name just need to be a extra timestamp column)
/// and `__ts_placeholder`(this column need to have exact this name and be a timestamp)
/// with values like `now()` and `0`
///
/// it also give existing columns alias to column in sink table if needed
#[derive(Debug)]
pub struct ColumnMatcherRewriter {
    pub schema: SchemaRef,
    pub is_rewritten: bool,
    pub primary_key_indices: Vec<usize>,
    pub allow_partial: bool,
}

impl ColumnMatcherRewriter {
    pub fn new(schema: SchemaRef, primary_key_indices: Vec<usize>, allow_partial: bool) -> Self {
        Self {
            schema,
            is_rewritten: false,
            primary_key_indices,
            allow_partial,
        }
    }

    /// modify the exprs in place so that it matches the schema and some auto columns are added
    fn modify_project_exprs(&mut self, mut exprs: Vec<Expr>) -> DfResult<Vec<Expr>> {
        if self.allow_partial {
            return self.modify_project_exprs_with_partial(exprs);
        }

        let all_names = self
            .schema
            .column_schemas()
            .iter()
            .map(|c| c.name.clone())
            .collect::<BTreeSet<_>>();
        // first match by position
        for (idx, expr) in exprs.iter_mut().enumerate() {
            if !all_names.contains(&expr.qualified_name().1)
                && let Some(col_name) = self
                    .schema
                    .column_schemas()
                    .get(idx)
                    .map(|c| c.name.clone())
            {
                // if the data type mismatched, later check_execute will error out
                // hence no need to check it here, beside, optimize pass might be able to cast it
                // so checking here is not necessary
                *expr = expr.clone().alias(col_name);
            }
        }

        // add columns if have different column count
        let query_col_cnt = exprs.len();
        let table_col_cnt = self.schema.column_schemas().len();
        debug!("query_col_cnt={query_col_cnt}, table_col_cnt={table_col_cnt}");

        let placeholder_ts_expr =
            datafusion::logical_expr::lit(ScalarValue::TimestampMillisecond(Some(0), None))
                .alias(AUTO_CREATED_PLACEHOLDER_TS_COL);

        if query_col_cnt == table_col_cnt {
            // still need to add alias, see below
        } else if query_col_cnt + 1 == table_col_cnt {
            let last_col_schema = self.schema.column_schemas().last().unwrap();

            // if time index column is auto created add it
            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index() == Some(table_col_cnt - 1)
            {
                exprs.push(placeholder_ts_expr);
            } else if last_col_schema.data_type.is_timestamp() {
                // is the update at column
                exprs.push(datafusion::prelude::now().alias(&last_col_schema.name));
            } else {
                // helpful error message
                return Err(DataFusionError::Plan(format!(
                    "Expect the last column in table to be timestamp column, found column {} with type {:?}",
                    last_col_schema.name, last_col_schema.data_type
                )));
            }
        } else if query_col_cnt + 2 == table_col_cnt {
            let mut col_iter = self.schema.column_schemas().iter().rev();
            let last_col_schema = col_iter.next().unwrap();
            let second_last_col_schema = col_iter.next().unwrap();
            if second_last_col_schema.data_type.is_timestamp() {
                exprs.push(datafusion::prelude::now().alias(&second_last_col_schema.name));
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect the second last column in the table to be timestamp column, found column {} with type {:?}",
                    second_last_col_schema.name, second_last_col_schema.data_type
                )));
            }

            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index() == Some(table_col_cnt - 1)
            {
                exprs.push(placeholder_ts_expr);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column {}, found {:?}",
                    AUTO_CREATED_PLACEHOLDER_TS_COL, last_col_schema
                )));
            }
        } else {
            return Err(DataFusionError::Plan(format!(
                "Expect table have 0,1 or 2 columns more than query columns, found {} query columns {:?}, {} table columns {:?}",
                query_col_cnt,
                exprs,
                table_col_cnt,
                self.schema.column_schemas()
            )));
        }
        Ok(exprs)
    }

    fn modify_project_exprs_with_partial(&mut self, exprs: Vec<Expr>) -> DfResult<Vec<Expr>> {
        let table_col_cnt = self.schema.column_schemas().len();
        let query_col_cnt = exprs.len();

        if query_col_cnt > table_col_cnt {
            return Err(DataFusionError::Plan(format!(
                "Expect query column count <= table column count, found {} query columns {:?}, {} table columns {:?}",
                query_col_cnt,
                exprs,
                table_col_cnt,
                self.schema.column_schemas()
            )));
        }

        let name_to_expr: HashMap<String, Expr> = exprs
            .clone()
            .into_iter()
            .map(|e| (e.qualified_name().1, e))
            .collect();

        let required_columns = self.required_columns_for_partial();
        let missing: Vec<_> = required_columns
            .iter()
            .filter(|name| !name_to_expr.contains_key(*name))
            .cloned()
            .collect();
        if !missing.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Column(s) {:?} required by sink table are missing from flow output when merge_mode=last_non_null",
                missing
            )));
        }

        let placeholder_ts_expr =
            datafusion::logical_expr::lit(ScalarValue::TimestampMillisecond(Some(0), None))
                .alias(AUTO_CREATED_PLACEHOLDER_TS_COL);

        let timestamp_index = self.schema.timestamp_index();
        let mut remap = name_to_expr;
        let mut new_exprs = Vec::with_capacity(table_col_cnt);

        for (idx, col_schema) in self.schema.column_schemas().iter().enumerate() {
            let col_name = col_schema.name.clone();
            if let Some(expr) = remap.remove(&col_name) {
                let expr = if expr.qualified_name().1 == col_name {
                    expr
                } else {
                    expr.alias(col_name.clone())
                };
                new_exprs.push(expr);
                continue;
            }

            if col_name == AUTO_CREATED_PLACEHOLDER_TS_COL && timestamp_index == Some(idx) {
                new_exprs.push(placeholder_ts_expr.clone());
                continue;
            }

            if col_name == AUTO_CREATED_UPDATE_AT_TS_COL && col_schema.data_type.is_timestamp() {
                new_exprs.push(datafusion::prelude::now().alias(&col_name));
                continue;
            }

            new_exprs.push(Self::null_expr(col_schema));
        }

        if !remap.is_empty() {
            let extra: Vec<_> = remap.keys().cloned().collect();
            return Err(DataFusionError::Plan(format!(
                "Flow output has extra column(s) {:?} not found in sink schema when merge_mode=last_non_null",
                extra
            )));
        }

        Ok(new_exprs)
    }

    fn null_expr(col_schema: &ColumnSchema) -> Expr {
        Expr::Literal(ScalarValue::Null, None).alias(col_schema.name.clone())
    }

    fn required_columns_for_partial(&self) -> HashSet<String> {
        let mut required = HashSet::new();
        for idx in &self.primary_key_indices {
            if let Some(col) = self.schema.column_schemas().get(*idx) {
                required.insert(col.name.clone());
            }
        }

        if let Some(ts_idx) = self.schema.timestamp_index()
            && let Some(col) = self.schema.column_schemas().get(ts_idx)
            && col.name != AUTO_CREATED_PLACEHOLDER_TS_COL
        {
            required.insert(col.name.clone());
        }

        required
    }
}

impl TreeNodeRewriter for ColumnMatcherRewriter {
    type Node = LogicalPlan;
    fn f_down(&mut self, mut node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }

        // if is distinct all, wrap it in a projection
        if let LogicalPlan::Distinct(Distinct::All(_)) = &node {
            let mut exprs = vec![];

            for field in node.schema().fields().iter() {
                exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(
                    field.name(),
                )));
            }

            let projection =
                LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(node.clone()))?);

            node = projection;
        }
        // handle table_scan by wrap it in a projection
        else if let LogicalPlan::TableScan(table_scan) = node {
            let mut exprs = vec![];

            for field in table_scan.projected_schema.fields().iter() {
                exprs.push(Expr::Column(datafusion::common::Column::new(
                    Some(table_scan.table_name.clone()),
                    field.name(),
                )));
            }

            let projection = LogicalPlan::Projection(Projection::try_new(
                exprs,
                Arc::new(LogicalPlan::TableScan(table_scan)),
            )?);

            node = projection;
        }

        // only do rewrite if found the outermost projection
        // if the outermost node is projection, can rewrite the exprs
        // if not, wrap it in a projection
        if let LogicalPlan::Projection(project) = &node {
            let exprs = project.expr.clone();
            let exprs = self.modify_project_exprs(exprs)?;

            self.is_rewritten = true;
            let new_plan =
                node.with_new_exprs(exprs, node.inputs().into_iter().cloned().collect())?;
            Ok(Transformed::yes(new_plan))
        } else {
            // wrap the logical plan in a projection
            let mut exprs = vec![];
            for field in node.schema().fields().iter() {
                exprs.push(Expr::Column(datafusion::common::Column::new_unqualified(
                    field.name(),
                )));
            }
            let exprs = self.modify_project_exprs(exprs)?;
            self.is_rewritten = true;
            let new_plan =
                LogicalPlan::Projection(Projection::try_new(exprs, Arc::new(node.clone()))?);
            Ok(Transformed::yes(new_plan))
        }
    }

    /// We might add new columns, so we need to recompute the schema
    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        node.recompute_schema().map(Transformed::yes)
    }
}

/// Find out the `Filter` Node corresponding to innermost(deepest) `WHERE` and add a new filter expr to it
#[derive(Debug)]
pub struct AddFilterRewriter {
    extra_filter: Expr,
    is_rewritten: bool,
}

impl AddFilterRewriter {
    pub fn new(filter: Expr) -> Self {
        Self {
            extra_filter: filter,
            is_rewritten: false,
        }
    }
}

impl TreeNodeRewriter for AddFilterRewriter {
    type Node = LogicalPlan;
    fn f_up(&mut self, node: Self::Node) -> DfResult<Transformed<Self::Node>> {
        if self.is_rewritten {
            return Ok(Transformed::no(node));
        }
        match node {
            LogicalPlan::Filter(mut filter) => {
                filter.predicate = filter.predicate.and(self.extra_filter.clone());
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            LogicalPlan::TableScan(_) => {
                // add a new filter
                let filter =
                    datafusion_expr::Filter::try_new(self.extra_filter.clone(), Arc::new(node))?;
                self.is_rewritten = true;
                Ok(Transformed::yes(LogicalPlan::Filter(filter)))
            }
            _ => Ok(Transformed::no(node)),
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_recordbatch::RecordBatch;
    use datafusion_common::tree_node::TreeNode as _;
    use datafusion_expr::GroupingSet;
    use datatypes::prelude::{ConcreteDataType, Scalar, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use pretty_assertions::assert_eq;
    use query::query_engine::DefaultSerializer;
    use session::context::QueryContext;
    use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
    use table::test_util::MemTable;

    use super::*;
    use crate::test_utils::create_test_query_engine;

    fn u32_table(table_name: &str, columns: Vec<&str>, rows: usize) -> TableRef {
        let column_schemas = columns
            .iter()
            .map(|name| ColumnSchema::new(*name, ConcreteDataType::uint32_datatype(), false))
            .collect::<Vec<_>>();
        let vectors = columns
            .iter()
            .map(|_| Arc::new(<u32 as Scalar>::VectorType::from_vec(vec![1; rows])) as VectorRef)
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(column_schemas));
        let recordbatch = RecordBatch::new(schema, vectors).unwrap();
        MemTable::table(table_name, recordbatch)
    }

    fn single_row_u32_table(table_name: &str, columns: Vec<&str>) -> TableRef {
        u32_table(table_name, columns, 1)
    }

    fn empty_u32_table(table_name: &str, columns: Vec<&str>) -> TableRef {
        u32_table(table_name, columns, 0)
    }

    fn assert_same_logical_plan(actual: &LogicalPlan, expected: &LogicalPlan) {
        assert_eq!(
            format!("{}", expected.display_indent()),
            format!("{}", actual.display_indent())
        );
    }

    fn test_sink_scan(sink_table: TableRef, sink_table_name: &TableName) -> LogicalPlan {
        let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        LogicalPlan::TableScan(
            TableScan::try_new(
                TableReference::Full {
                    catalog: sink_table_name[0].clone().into(),
                    schema: sink_table_name[1].clone().into(),
                    table: sink_table_name[2].clone().into(),
                },
                table_source,
                None,
                vec![],
                None,
            )
            .unwrap(),
        )
    }

    fn expected_left_join_rewrite(
        delta_plan: &LogicalPlan,
        sink_table: TableRef,
        sink_table_name: &TableName,
        delta_selected_exprs: Vec<Expr>,
        sink_selected_exprs: Vec<Expr>,
        join_keys: (Vec<Column>, Vec<Column>),
        projection_exprs: Vec<Expr>,
    ) -> LogicalPlan {
        let delta_alias = "__flow_delta";
        let sink_alias = "__flow_sink";
        let delta_selected = LogicalPlanBuilder::from(delta_plan.clone())
            .project(delta_selected_exprs)
            .unwrap()
            .alias(delta_alias)
            .unwrap()
            .build()
            .unwrap();
        let sink_selected = LogicalPlanBuilder::from(test_sink_scan(sink_table, sink_table_name))
            .project(sink_selected_exprs)
            .unwrap()
            .alias(sink_alias)
            .unwrap()
            .build()
            .unwrap();
        let joined = LogicalPlanBuilder::from(delta_selected)
            .join_detailed(
                sink_selected,
                JoinType::Left,
                join_keys,
                None,
                NullEquality::NullEqualsNull,
            )
            .unwrap()
            .build()
            .unwrap();
        LogicalPlanBuilder::from(joined)
            .project(projection_exprs)
            .unwrap()
            .build()
            .unwrap()
    }

    fn max_merge_expr(field_name: &str) -> Expr {
        let left = qualified_col("__flow_delta", field_name);
        let right = qualified_col("__flow_sink", field_name);
        when(is_null(right.clone()), left.clone())
            .when(left.clone().gt_eq(right.clone()), left)
            .otherwise(right)
            .unwrap()
            .alias(field_name)
    }

    fn sum_merge_expr(field_name: &str) -> Expr {
        let left = qualified_col("__flow_delta", field_name);
        let right = qualified_col("__flow_sink", field_name);
        when(is_null(left.clone()), right.clone())
            .when(is_null(right.clone()), left.clone())
            .otherwise(binary_expr(left, Operator::Plus, right))
            .unwrap()
            .alias(field_name)
    }

    async fn analyze_test_sql(sql: &str) -> IncrementalAggregateAnalysis {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
        analyze_incremental_aggregate_plan(&plan).unwrap().unwrap()
    }

    async fn analyze_grouping_set_plan(
        make_grouping_set: impl FnOnce(Expr) -> GroupingSet,
    ) -> IncrementalAggregateAnalysis {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let plan = sql_to_df_plan(
            ctx,
            query_engine,
            "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
            false,
        )
        .await
        .unwrap();

        let LogicalPlan::Projection(projection) = plan else {
            panic!("expected projection over aggregate")
        };
        let LogicalPlan::Aggregate(aggregate) = projection.input.as_ref() else {
            panic!("expected aggregate below projection")
        };
        let group_expr = aggregate.group_expr[0].clone();
        let grouping_set_aggregate = datafusion_expr::logical_plan::Aggregate::try_new(
            aggregate.input.clone(),
            vec![Expr::GroupingSet(make_grouping_set(group_expr))],
            aggregate.aggr_expr.clone(),
        )
        .unwrap();
        let plan = LogicalPlan::Aggregate(grouping_set_aggregate);
        analyze_incremental_aggregate_plan(&plan).unwrap().unwrap()
    }

    fn assert_unsupported(analysis: &IncrementalAggregateAnalysis, reason: &str) {
        assert!(
            analysis
                .unsupported_exprs
                .iter()
                .any(|expr| expr.contains(reason)),
            "expected unsupported reason containing {reason:?}, got {:?}",
            analysis.unsupported_exprs
        );
        assert!(
            analysis.merge_columns.is_empty(),
            "unsupported analysis should disable merge columns"
        );
    }

    /// test if uppercase are handled correctly(with quote)
    #[tokio::test]
    async fn test_sql_plan_convert() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let old = r#"SELECT "NUMBER" FROM "UPPERCASE_NUMBERS_WITH_TS""#;
        let new = sql_to_df_plan(ctx.clone(), query_engine.clone(), old, false)
            .await
            .unwrap();
        let new_sql = df_plan_to_sql(&new).unwrap();

        assert_eq!(
            r#"SELECT `UPPERCASE_NUMBERS_WITH_TS`.`NUMBER` FROM `UPPERCASE_NUMBERS_WITH_TS`"#,
            new_sql
        );
    }

    #[tokio::test]
    async fn test_add_filter() {
        let testcases = vec![
            (
                "SELECT number FROM numbers_with_ts GROUP BY number",
                "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE (number > 4) GROUP BY numbers_with_ts.number",
            ),
            (
                "SELECT number FROM numbers_with_ts WHERE number < 2 OR number >10",
                "SELECT numbers_with_ts.number FROM numbers_with_ts WHERE ((numbers_with_ts.number < 2) OR (numbers_with_ts.number > 10)) AND (number > 4)",
            ),
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
                "SELECT date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts)",
            ),
            // subquery
            (
                "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);",
                "SELECT numbers_with_ts.number, time_window FROM (SELECT numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window FROM numbers_with_ts WHERE (number > 4) GROUP BY date_bin('5 minutes', numbers_with_ts.ts), numbers_with_ts.number)",
            ),
            // complex subquery without alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
                "SELECT sum(numbers_with_ts.number), numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts) AS time_window, bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) GROUP BY numbers_with_ts.number, date_bin('5 minutes', numbers_with_ts.ts), bucket_name",
            ),
            // complex subquery alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte WHERE number > 1 GROUP BY number, time_window, bucket_name;",
                "SELECT sum(cte.number), cte.number, date_bin('5 minutes', cte.ts) AS time_window, cte.bucket_name FROM (SELECT numbers_with_ts.number, numbers_with_ts.ts, CASE WHEN (numbers_with_ts.number < 5) THEN 'bucket_0_5' WHEN (numbers_with_ts.number >= 5) THEN 'bucket_5_inf' END AS bucket_name FROM numbers_with_ts WHERE (number > 4)) AS cte WHERE (cte.number > 1) GROUP BY cte.number, date_bin('5 minutes', cte.ts), cte.bucket_name",
            ),
        ];
        use datafusion_expr::{col, lit};
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();

        for (before, after) in testcases {
            let sql = before;
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();

            let mut add_filter = AddFilterRewriter::new(col("number").gt(lit(4u32)));
            let plan = plan.rewrite(&mut add_filter).unwrap().data;
            let new_sql = df_plan_to_sql(&plan).unwrap();
            assert_eq!(after, new_sql);
        }
    }

    #[tokio::test]
    async fn test_add_auto_column_rewriter() {
        let testcases = vec![
            // add update_at
            (
                "SELECT number FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, now() AS ts FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add ts placeholder
            (
                "SELECT number FROM numbers_with_ts",
                Ok(
                    "SELECT numbers_with_ts.number, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // no modify
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok("SELECT numbers_with_ts.number, numbers_with_ts.ts FROM numbers_with_ts"),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add update_at and ts placeholder
            (
                "SELECT number FROM numbers_with_ts",
                Ok(
                    "SELECT numbers_with_ts.number, now() AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "update_at",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add ts placeholder
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok(
                    "SELECT numbers_with_ts.number, numbers_with_ts.ts AS update_at, CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS __ts_placeholder FROM numbers_with_ts",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "update_at",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                    ColumnSchema::new(
                        AUTO_CREATED_PLACEHOLDER_TS_COL,
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
            // add update_at after time index column
            (
                "SELECT number, ts FROM numbers_with_ts",
                Ok(
                    "SELECT numbers_with_ts.number, numbers_with_ts.ts, now() AS update_atat FROM numbers_with_ts",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "update_atat",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    ),
                ],
            ),
            // error datatype mismatch
            (
                "SELECT number, ts FROM numbers_with_ts",
                Err(
                    "Expect the last column in table to be timestamp column, found column atat with type Int8",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new(
                        "ts",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "atat",
                        ConcreteDataType::int8_datatype(),
                        false,
                    ),
                ],
            ),
            // error datatype mismatch on second last column
            (
                "SELECT number FROM numbers_with_ts",
                Err(
                    "Expect the second last column in the table to be timestamp column, found column ts with type Int8",
                ),
                vec![
                    ColumnSchema::new("number", ConcreteDataType::int32_datatype(), true),
                    ColumnSchema::new("ts", ConcreteDataType::int8_datatype(), false),
                    ColumnSchema::new(
                        // name is irrelevant for update_at column
                        "atat",
                        ConcreteDataType::timestamp_millisecond_datatype(),
                        false,
                    )
                    .with_time_index(true),
                ],
            ),
        ];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for (before, after, column_schemas) in testcases {
            let schema = Arc::new(Schema::new(column_schemas));
            let mut add_auto_column_rewriter =
                ColumnMatcherRewriter::new(schema, Vec::new(), false);

            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), before, false)
                .await
                .unwrap();
            let new_sql = (|| {
                let plan = plan
                    .rewrite(&mut add_auto_column_rewriter)
                    .map_err(|e| e.to_string())?
                    .data;
                df_plan_to_sql(&plan).map_err(|e| e.to_string())
            })();
            match (after, new_sql.clone()) {
                (Ok(after), Ok(new_sql)) => assert_eq!(after, new_sql),
                (Err(expected), Err(real_err_msg)) => assert!(
                    real_err_msg.contains(expected),
                    "expected: {expected}, real: {real_err_msg}"
                ),
                _ => panic!("expected: {:?}, real: {:?}", after, new_sql),
            }
        }
    }

    #[tokio::test]
    async fn test_find_group_by_exprs() {
        let testcases = vec![
            (
                "SELECT arrow_cast(date_bin(INTERVAL '1 MINS', numbers_with_ts.ts), 'Timestamp(Second, None)') AS ts FROM numbers_with_ts GROUP BY ts;",
                vec!["ts"],
            ),
            (
                "SELECT number FROM numbers_with_ts GROUP BY number",
                vec!["number"],
            ),
            (
                "SELECT date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window",
                vec!["time_window"],
            ),
            // subquery
            (
                "SELECT number, time_window FROM (SELECT number, date_bin('5 minutes', ts) as time_window FROM numbers_with_ts GROUP BY time_window, number);",
                vec!["time_window", "number"],
            ),
            // complex subquery without alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) GROUP BY number, time_window, bucket_name;",
                vec!["number", "time_window", "bucket_name"],
            ),
            // complex subquery alias
            (
                "SELECT sum(number), number, date_bin('5 minutes', ts) as time_window, bucket_name FROM (SELECT number, ts, case when number < 5 THEN 'bucket_0_5' when number >= 5 THEN 'bucket_5_inf' END as bucket_name FROM numbers_with_ts) as cte GROUP BY number, time_window, bucket_name;",
                vec!["number", "time_window", "bucket_name"],
            ),
        ];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for (sql, expected) in testcases {
            // need to be unoptimize for better readiability
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let mut group_by_exprs = FindGroupByFinalName::default();
            plan.visit(&mut group_by_exprs).unwrap();
            let expected: HashSet<String> = expected.into_iter().map(|s| s.to_string()).collect();
            assert_eq!(
                expected,
                group_by_exprs.get_group_expr_names().unwrap_or_default()
            );
        }
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let testcases: Vec<(&str, IncrementalAggregateMergeOp, &str)> = vec![
            (
                "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::Sum,
                "number",
            ),
            (
                "SELECT count(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::Sum,
                "number",
            ),
            (
                "SELECT min(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::Min,
                "number",
            ),
            (
                "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::Max,
                "number",
            ),
            (
                "SELECT bit_and(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::BitAnd,
                "number",
            ),
            (
                "SELECT bit_or(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::BitOr,
                "number",
            ),
            (
                "SELECT bit_xor(number) AS number, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::BitXor,
                "number",
            ),
            (
                "SELECT bool_and(number > 5) AS cond, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::BoolAnd,
                "cond",
            ),
            (
                "SELECT bool_or(number > 5) AS cond, ts FROM numbers_with_ts GROUP BY ts",
                IncrementalAggregateMergeOp::BoolOr,
                "cond",
            ),
        ];

        for (sql, expected_op, expected_field_name) in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();

            let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
            assert!(analysis.unsupported_exprs.is_empty());
            assert!(analysis.group_key_names.contains(&"ts".to_string()));
            assert_eq!(analysis.merge_columns.len(), 1);
            assert_eq!(
                analysis.merge_columns[0].output_field_name,
                expected_field_name
            );
            assert_eq!(analysis.merge_columns[0].merge_op, expected_op);
        }
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_keeps_aliases_for_multiple_aggregates() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS max_number, min(number) AS min_number, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert_eq!(analysis.merge_columns.len(), 2);
        assert!(analysis.merge_columns.iter().any(|merge_col| {
            merge_col.output_field_name == "max_number"
                && merge_col.merge_op == IncrementalAggregateMergeOp::Max
        }));
        assert!(analysis.merge_columns.iter().any(|merge_col| {
            merge_col.output_field_name == "min_number"
                && merge_col.merge_op == IncrementalAggregateMergeOp::Min
        }));
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_allows_where_before_aggregate() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql =
            "SELECT sum(number) AS number, ts FROM numbers_with_ts WHERE number > 10 GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(analysis.merge_columns[0].output_field_name, "number");
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_having_filter() {
        let sql = "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts HAVING sum(number) > 10";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "post-aggregate filter");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_allows_aggregate_filter() {
        let sql = "SELECT sum(number) FILTER (WHERE number > 10) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let analysis = analyze_test_sql(sql).await;

        assert!(analysis.unsupported_exprs.is_empty());
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(analysis.merge_columns[0].output_field_name, "number");
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_aggregate_order_by() {
        let sql = "SELECT sum(number ORDER BY ts) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "aggregate ORDER BY");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_sort_above_aggregate() {
        let sql = "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts ORDER BY number DESC";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "post-aggregate plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_limit_above_aggregate() {
        let sql = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts LIMIT 1";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "post-aggregate plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_distinct_above_aggregate() {
        let sql = "SELECT DISTINCT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "post-aggregate plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_nested_aggregates() {
        let sql = "SELECT sum(cnt) AS total FROM (SELECT count(*) AS cnt, ts FROM numbers_with_ts GROUP BY ts) s";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "Aggregate nodes");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_union_aggregate_branches() {
        let sql = "SELECT sum(number) AS number, ts FROM numbers_with_ts GROUP BY ts UNION ALL SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "Aggregate nodes");
        assert_unsupported(&analysis, "post-aggregate plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_window_above_aggregate() {
        let sql = "SELECT sum(number) AS number, ts, row_number() OVER (ORDER BY sum(number)) AS rn FROM numbers_with_ts GROUP BY ts";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "post-aggregate plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_join_below_aggregate() {
        let sql = "SELECT sum(lhs.number) AS number, lhs.ts FROM numbers_with_ts AS lhs JOIN numbers_with_ts AS rhs ON lhs.ts = rhs.ts GROUP BY lhs.ts";
        let analysis = analyze_test_sql(sql).await;
        assert_unsupported(&analysis, "aggregate input plan shape");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_grouping_sets() {
        let analysis =
            analyze_grouping_set_plan(|expr| GroupingSet::GroupingSets(vec![vec![expr]])).await;
        assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_cube() {
        let analysis = analyze_grouping_set_plan(|expr| GroupingSet::Cube(vec![expr])).await;
        assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_rollup() {
        let analysis = analyze_grouping_set_plan(|expr| GroupingSet::Rollup(vec![expr])).await;
        assert_unsupported(&analysis, "GROUPING SETS/CUBE/ROLLUP");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_preserves_raw_aggregate_name() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number), ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(
            analysis.merge_columns[0].output_field_name,
            "max(numbers_with_ts.number)"
        );
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Max
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_wrapper_aliased_as_raw_name() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = r#"SELECT COALESCE(max(number), 0) AS "max(numbers_with_ts.number)", ts FROM numbers_with_ts GROUP BY ts"#;
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            !analysis.unsupported_exprs.is_empty(),
            "wrapper aliased to a raw aggregate field name must not bypass analysis"
        );
        assert!(analysis.merge_columns.is_empty());
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_supports_count_star() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT count(*) AS wildcard, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(analysis.merge_columns[0].output_field_name, "wildcard");
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_supports_aggregate_input_exprs() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let testcases = [
            "SELECT sum(abs(number)) AS sum_abs, ts FROM numbers_with_ts GROUP BY ts",
            "SELECT sum(CASE WHEN number > 5 THEN 1 ELSE 0 END) AS above_five, ts FROM numbers_with_ts GROUP BY ts",
        ];

        for sql in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
            assert!(
                analysis.unsupported_exprs.is_empty(),
                "aggregate input expressions should be mergeable for SQL: {sql}"
            );
            assert_eq!(analysis.merge_columns.len(), 1);
            assert_eq!(
                analysis.merge_columns[0].merge_op,
                IncrementalAggregateMergeOp::Sum
            );
        }
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_output_expr_wrappers() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let testcases = [
            "SELECT abs(sum(number)) AS abs_sum, ts FROM numbers_with_ts GROUP BY ts",
            "SELECT max(number) - min(number) AS maxmin, ts FROM numbers_with_ts GROUP BY ts",
            "SELECT count(number) + 123 AS total_count, ts FROM numbers_with_ts GROUP BY ts",
            "SELECT sum(CASE WHEN number > 5 THEN 1 ELSE 0 END) / count(number) AS ratio, ts FROM numbers_with_ts GROUP BY ts",
        ];

        for sql in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
            assert!(
                !analysis.unsupported_exprs.is_empty(),
                "aggregate output wrappers should be rejected for SQL: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_allows_literal_outputs() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS number, ts, 42 AS lit FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(analysis.literal_columns, vec!["lit".to_string()]);
        assert_eq!(
            analysis.output_field_names,
            vec!["number".to_string(), "ts".to_string(), "lit".to_string()]
        );

        let sink_table_name = [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ];
        let (sink_table, _) = get_table_info_df_schema(
            query_engine.engine_state().catalog_manager().clone(),
            sink_table_name.clone(),
        )
        .await
        .unwrap();
        let rewritten = rewrite_incremental_aggregate_with_sink_merge(
            &plan,
            &analysis,
            sink_table.clone(),
            &sink_table_name,
        )
        .await
        .unwrap();

        let rewritten_fields = rewritten
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert_eq!(rewritten_fields, analysis.output_field_names);
        let expected = expected_left_join_rewrite(
            &plan,
            sink_table,
            &sink_table_name,
            vec![
                unqualified_col("ts"),
                unqualified_col("number"),
                unqualified_col("lit"),
            ],
            vec![unqualified_col("ts"), unqualified_col("number")],
            (
                vec![qualified_column("__flow_delta", "ts")],
                vec![qualified_column("__flow_sink", "ts")],
            ),
            vec![
                max_merge_expr("number"),
                qualified_col("__flow_delta", "ts").alias("ts"),
                qualified_col("__flow_delta", "lit").alias("lit"),
            ],
        );
        assert_same_logical_plan(&rewritten, &expected);
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_allows_unaliased_literal_output() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT 42, max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(analysis.literal_columns.len(), 1);
        assert_eq!(analysis.output_field_names[0], analysis.literal_columns[0]);
        assert_eq!(analysis.output_field_names[1], "number");
        assert_eq!(analysis.output_field_names[2], "ts");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_allows_string_literal_output() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql =
            "SELECT max(number) AS number, ts, 'hello' AS label FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(analysis.literal_columns, vec!["label".to_string()]);
        assert_eq!(
            analysis.output_field_names,
            vec!["number".to_string(), "ts".to_string(), "label".to_string()]
        );
    }

    #[tokio::test]
    async fn test_rewrite_incremental_aggregate_preserves_non_identifier_aliases() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS \"max value\", number, 42 AS \"literal value\" FROM numbers_with_ts GROUP BY number";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert_eq!(
            analysis.output_field_names,
            vec!["max value", "number", "literal value"]
        );

        let sink_table =
            single_row_u32_table("non_identifier_alias_sink", vec!["number", "max value"]);
        let rewritten = rewrite_incremental_aggregate_with_sink_merge(
            &plan,
            &analysis,
            sink_table,
            &[
                "greptime".to_string(),
                "public".to_string(),
                "non_identifier_alias_sink".to_string(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            rewritten
                .schema()
                .fields()
                .iter()
                .map(|field| field.name().clone())
                .collect::<Vec<_>>(),
            vec![
                "max value".to_string(),
                "number".to_string(),
                "literal value".to_string()
            ]
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_uncovered_outputs() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT sum(number) AS total, number + 1 AS bucket FROM numbers_with_ts GROUP BY number";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            analysis
                .unsupported_exprs
                .iter()
                .any(|expr| expr.contains("unsupported output field: bucket")),
            "non-literal extra output should be rejected: {:?}",
            analysis.unsupported_exprs
        );
    }

    #[tokio::test]
    async fn test_datafusion_rejects_duplicate_output_names() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS x, min(number) AS x, ts FROM numbers_with_ts GROUP BY ts";
        let err = sql_to_df_plan(ctx, query_engine, sql, false)
            .await
            .unwrap_err();
        let err = format!("{err:?}");
        assert!(
            err.contains("Projections require unique expression names"),
            "DataFusion should reject duplicate output aliases before incremental analysis: {err}"
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_same_aggregate_multiple_aliases() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT sum(number) AS a, sum(number) AS b, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(
            analysis
                .unsupported_exprs
                .iter()
                .any(|expr| expr.contains("same aggregate output")
                    && expr.contains("a")
                    && expr.contains("b")),
            "same aggregate with multiple aliases should be unsupported until explicit reproduction is implemented: {:?}",
            analysis.unsupported_exprs
        );
        assert!(analysis.merge_columns.is_empty());
    }

    #[test]
    fn test_qualified_col_preserves_non_identifier_field_name() {
        let expr = qualified_col("__flow_delta", "max(numbers_with_ts.number)");
        let Expr::Column(column) = expr else {
            panic!("expected column expression");
        };
        assert_eq!(column.name, "max(numbers_with_ts.number)");
        assert_eq!(column.relation.unwrap().to_string(), "__flow_delta");
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_multiple_group_keys() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT sum(number) AS total, ts, number AS bucket FROM numbers_with_ts GROUP BY ts, number";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());
        assert!(analysis.group_key_names.contains(&"ts".to_string()));
        assert!(analysis.group_key_names.contains(&"bucket".to_string()));
        assert_eq!(analysis.group_key_names.len(), 2);
        assert_eq!(analysis.merge_columns.len(), 1);
        assert_eq!(analysis.merge_columns[0].output_field_name, "total");
        assert_eq!(
            analysis.merge_columns[0].merge_op,
            IncrementalAggregateMergeOp::Sum
        );
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_avg() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT avg(number) AS avg_num, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(!analysis.unsupported_exprs.is_empty());
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_distinct() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT count(distinct number) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(!analysis.unsupported_exprs.is_empty());
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_coalesce_wrapped_aggregate() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        // COALESCE wraps the aggregate output — the wrapper is not merge-transparent,
        // so the analyzer should mark the aggregate as unsupported rather than
        // attempting an unsafe incremental rewrite.
        let sql =
            "SELECT COALESCE(max(number), 0) AS coalesced_max, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        // Non-transparent wrapper → alias unresolvable → unsupported
        assert!(
            !analysis.unsupported_exprs.is_empty(),
            "COALESCE-wrapped aggregate should be unsupported"
        );
        assert!(
            analysis.merge_columns.is_empty(),
            "COALESCE-wrapped aggregate should have no merge columns"
        );
    }

    #[tokio::test]
    async fn test_rewrite_incremental_aggregate_with_left_join() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
        let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
            .await
            .unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        let sink_table_name = [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ];
        let (sink_table, _) = get_table_info_df_schema(
            query_engine.engine_state().catalog_manager().clone(),
            sink_table_name.clone(),
        )
        .await
        .unwrap();

        let rewritten = rewrite_incremental_aggregate_with_sink_merge(
            &plan,
            &analysis,
            sink_table.clone(),
            &sink_table_name,
        )
        .await
        .unwrap();

        let expected = expected_left_join_rewrite(
            &plan,
            sink_table,
            &sink_table_name,
            vec![unqualified_col("ts"), unqualified_col("number")],
            vec![unqualified_col("ts"), unqualified_col("number")],
            (
                vec![qualified_column("__flow_delta", "ts")],
                vec![qualified_column("__flow_sink", "ts")],
            ),
            vec![
                max_merge_expr("number"),
                qualified_col("__flow_delta", "ts").alias("ts"),
            ],
        );
        assert_same_logical_plan(&rewritten, &expected);
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_global_aggregate() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let testcases = [
            "SELECT max(number) AS number FROM numbers_with_ts",
            "SELECT max(number) AS number, 42 AS lit FROM numbers_with_ts",
            "SELECT count(*) AS cnt, sum(number) AS total FROM numbers_with_ts",
        ];

        for sql in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
            assert_unsupported(&analysis, "global aggregate");
        }
    }

    #[tokio::test]
    async fn test_rewrite_incremental_aggregate_rejects_empty_group_keys() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number) AS number FROM numbers_with_ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
        let analysis = IncrementalAggregateAnalysis {
            group_key_names: vec![],
            merge_columns: vec![IncrementalAggregateMergeColumn::new(
                "number".to_string(),
                IncrementalAggregateMergeOp::Max,
            )],
            literal_columns: vec![],
            output_field_names: vec!["number".to_string()],
            unsupported_exprs: vec![],
        };

        let sink_table = single_row_u32_table("global_guard_sink", vec!["number"]);
        let sink_table_name = [
            "greptime".to_string(),
            "public".to_string(),
            "global_guard_sink".to_string(),
        ];
        let err = rewrite_incremental_aggregate_with_sink_merge(
            &plan,
            &analysis,
            sink_table,
            &sink_table_name,
        )
        .await
        .unwrap_err();
        let err = format!("{err:?}");
        assert!(
            err.contains("global aggregate query is not supported"),
            "rewrite should defensively reject empty group keys: {err}"
        );
    }

    #[tokio::test]
    async fn test_rewrite_incremental_aggregate_preserves_raw_aggregate_field_name() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT max(number), number FROM numbers_with_ts GROUP BY number";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();
        let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
        assert!(analysis.unsupported_exprs.is_empty());

        let raw_field_name = "max(numbers_with_ts.number)";
        let sink_table = single_row_u32_table("raw_aggregate_sink", vec!["number", raw_field_name]);
        let sink_table_name = [
            "greptime".to_string(),
            "public".to_string(),
            "raw_aggregate_sink".to_string(),
        ];
        let rewritten = rewrite_incremental_aggregate_with_sink_merge(
            &plan,
            &analysis,
            sink_table.clone(),
            &sink_table_name,
        )
        .await
        .unwrap();

        let rewritten_fields = rewritten
            .schema()
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect::<Vec<_>>();
        assert!(rewritten_fields.contains(&raw_field_name.to_string()));
        let expected = expected_left_join_rewrite(
            &plan,
            sink_table,
            &sink_table_name,
            vec![unqualified_col("number"), unqualified_col(raw_field_name)],
            vec![unqualified_col("number"), unqualified_col(raw_field_name)],
            (
                vec![qualified_column("__flow_delta", "number")],
                vec![qualified_column("__flow_sink", "number")],
            ),
            vec![
                max_merge_expr(raw_field_name),
                qualified_col("__flow_delta", "number").alias("number"),
            ],
        );
        assert_same_logical_plan(&rewritten, &expected);
    }

    #[tokio::test]
    async fn test_null_cast() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let sql = "SELECT NULL::DOUBLE FROM numbers_with_ts";
        let plan = sql_to_df_plan(ctx, query_engine.clone(), sql, false)
            .await
            .unwrap();

        let _sub_plan = DFLogicalSubstraitConvertor {}
            .encode(&plan, DefaultSerializer)
            .unwrap();
    }

    #[tokio::test]
    async fn test_analyze_incremental_aggregate_plan_rejects_cast_wrapped_alias() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        let testcases = [
            "SELECT CAST(sum(number) AS BIGINT) AS total, ts FROM numbers_with_ts GROUP BY ts",
            "SELECT TRY_CAST(sum(number) AS BIGINT) AS total, ts FROM numbers_with_ts GROUP BY ts",
        ];

        for sql in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), sql, false)
                .await
                .unwrap();
            let analysis = analyze_incremental_aggregate_plan(&plan).unwrap().unwrap();
            assert!(
                !analysis.unsupported_exprs.is_empty(),
                "CAST/TryCast-wrapped aggregate output should be unsupported for SQL: {sql}"
            );
        }
    }

    #[tokio::test]
    async fn test_aggregate_expr_finder_counts_multiple_aggregates() {
        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        // Subquery has an inner aggregate (count), outer query has another aggregate (sum).
        let sql = "SELECT sum(cnt) AS total, ts \
            FROM (SELECT ts, count(number) AS cnt FROM numbers_with_ts GROUP BY ts) AS sub \
            GROUP BY ts";
        let plan = sql_to_df_plan(ctx, query_engine, sql, false).await.unwrap();

        let mut finder = AggregateExprFinder::default();
        plan.visit(&mut finder).unwrap();
        assert!(
            finder.aggregate_count > 1,
            "nested aggregate plans should be identifiable as unsupported"
        );
    }
}
