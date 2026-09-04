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

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_function::aggrs::aggr_wrapper::get_aggr_func;
use common_telemetry::debug;
use datafusion::arrow::datatypes::DataType as ArrowDataType;
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
use datafusion_expr::logical_plan::{Aggregate, TableScan};
use datafusion_expr::{
    Distinct, ExprSchemable, JoinType, LogicalPlan, LogicalPlanBuilder, Operator, Projection, and,
    binary_expr, bitwise_and, bitwise_or, bitwise_xor, is_null, or, when,
};
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
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

#[cfg(test)]
mod test;

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
    /// Delta-plan field containing the aggregate result/state column. Repeated
    /// projections intentionally share this field while using distinct outputs.
    pub input_field_name: String,
    /// Final output/sink field name for the aggregate result/state column.
    ///
    pub output_field_name: String,
    pub merge_op: IncrementalAggregateMergeOp,
}

impl IncrementalAggregateMergeColumn {
    /// Create a new merge column whose delta and output fields have the same name.
    pub fn new(output_field_name: String, merge_op: IncrementalAggregateMergeOp) -> Self {
        Self {
            input_field_name: output_field_name.clone(),
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
    AvgDeltaMerge,
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

fn has_grouping_set(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(aggregate) => aggregate
            .group_expr
            .iter()
            .any(|expr| matches!(expr, Expr::GroupingSet(_))),
        _ => plan.inputs().into_iter().any(has_grouping_set),
    }
}

fn has_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        _ => plan.inputs().into_iter().any(has_aggregate),
    }
}

fn peel_subquery_aliases(mut plan: &LogicalPlan) -> &LogicalPlan {
    while let LogicalPlan::SubqueryAlias(alias) = plan {
        plan = alias.input.as_ref();
    }
    plan
}

fn extract_incremental_aggregate(plan: &LogicalPlan) -> Result<Option<&Aggregate>, String> {
    // Supported final shape: optional output Projection directly over one
    // Aggregate. Post-aggregate filters (HAVING), ordering, limits,
    // distinct/window/union/extension nodes are intentionally not accepted.
    let plan = match plan {
        LogicalPlan::Projection(projection) => projection.input.as_ref(),
        _ => plan,
    };

    match plan {
        LogicalPlan::Aggregate(aggregate) => {
            check_input_plan_shape(aggregate.input.as_ref())?;
            Ok(Some(aggregate))
        }
        LogicalPlan::Filter(filter) if has_aggregate(filter.input.as_ref()) => Err(
            "unsupported post-aggregate filter (HAVING) in incremental aggregate rewrite"
                .to_string(),
        ),
        _ if has_aggregate(plan) => Err(
            "unsupported post-aggregate plan shape in incremental aggregate rewrite".to_string(),
        ),
        _ => Ok(None),
    }
}

fn check_input_plan_shape(plan: &LogicalPlan) -> Result<(), String> {
    let plan = peel_subquery_aliases(plan);
    match plan {
        // Supported aggregate input shape: optional WHERE filter over a table scan.
        // SubqueryAlias is a transparent naming wrapper for `FROM table AS alias`.
        LogicalPlan::TableScan(_) => Ok(()),
        LogicalPlan::Filter(filter) => match peel_subquery_aliases(filter.input.as_ref()) {
            LogicalPlan::TableScan(_) => Ok(()),
            _ => Err(
                "unsupported aggregate input plan shape in incremental aggregate rewrite"
                    .to_string(),
            ),
        },
        _ => Err(
            "unsupported aggregate input plan shape in incremental aggregate rewrite".to_string(),
        ),
    }
}

#[derive(Debug, Default)]
struct OutputProjectionInfo {
    has_top_level_projection: bool,
    /// Aggregate expression name and projected output field, in projection order.
    aggregate_outputs: Vec<(String, String)>,
    /// Original single-instance resolver mapping, retained for compatibility.
    output_aliases: HashMap<String, String>,
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
                    // Only a direct aggregate output column has the same
                    // merge semantics as the original resolver. In particular,
                    // do not mine aggregate columns through CAST/TryCast or
                    // other output wrappers.
                    let alias_name = alias.name.clone();
                    if let Expr::Column(column) = alias.expr.as_ref() {
                        output_aliases
                            .entry(column.name.clone())
                            .or_insert_with(|| alias_name.clone());
                        projection_info
                            .aggregate_outputs
                            .push((column.name.clone(), alias_name));
                    } else if let Expr::Alias(inner_alias) = alias.expr.as_ref()
                        && inner_alias.name.eq_ignore_ascii_case("count(*)")
                        && let Expr::Column(column) = inner_alias.expr.as_ref()
                    {
                        output_aliases
                            .entry(column.name.clone())
                            .or_insert_with(|| alias_name.clone());
                        projection_info
                            .aggregate_outputs
                            .push((column.name.clone(), alias_name));
                    } else if is_passthrough_output_column(&alias_name, alias.expr.as_ref()) {
                        projection_info.literal_columns.insert(alias_name);
                    }
                }
                Expr::Column(col) => {
                    projection_info
                        .aggregate_outputs
                        .push((col.name.clone(), col.name.clone()));
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

    if projection_info
        .output_field_names
        .iter()
        .any(|name| name == AUTO_CREATED_PLACEHOLDER_TS_COL)
    {
        projection_info
            .literal_columns
            .insert(AUTO_CREATED_PLACEHOLDER_TS_COL.to_string());
    }

    projection_info.output_aliases = output_aliases;
    projection_info
}

fn is_passthrough_output_column(alias_name: &str, expr: &Expr) -> bool {
    matches!(expr, Expr::Literal(_, _))
        || match alias_name {
            AUTO_CREATED_UPDATE_AT_TS_COL => expr == &datafusion::prelude::now(),
            AUTO_CREATED_PLACEHOLDER_TS_COL => is_literal_or_cast_literal(expr),
            _ => false,
        }
}

fn is_literal_or_cast_literal(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(_, _) => true,
        Expr::Cast(cast) => is_literal_or_cast_literal(cast.expr.as_ref()),
        Expr::TryCast(cast) => is_literal_or_cast_literal(cast.expr.as_ref()),
        _ => false,
    }
}

fn merge_op_for_aggregate_expr(
    aggr_expr: &Expr,
    input_schema: &DFSchema,
) -> Result<IncrementalAggregateMergeOp, String> {
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
        "avg_state" => match aggr_func.params.args.as_slice() {
            [_] => Ok(IncrementalAggregateMergeOp::AvgDeltaMerge),
            _ => Err(aggr_expr.to_string()),
        },
        "avg_merge" => match aggr_func.params.args.as_slice() {
            [arg] if arg.get_type(input_schema).ok() == Some(ArrowDataType::Binary) => {
                Ok(IncrementalAggregateMergeOp::AvgDeltaMerge)
            }
            _ => Err(aggr_expr.to_string()),
        },
        _ => Err(aggr_expr.to_string()),
    }
}

fn resolve_aggregate_output_fields(
    aggr_expr: &Expr,
    projection_info: &OutputProjectionInfo,
    output_field_name_set: &HashSet<String>,
) -> Vec<(String, String)> {
    // qualified_name() returns (Option<String>, String) where the second
    // element is the unqualified column/alias name. This relies on
    // DataFusion's internal naming convention: aggregate expressions
    // emit a column named after the aggregate itself (e.g. "SUM(x)").
    // Keep every matching projection occurrence because DataFusion can share
    // one aggregate input field for identical expressions.
    let raw_name = aggr_expr.qualified_name().1;
    if projection_info.has_top_level_projection {
        let outputs = projection_info
            .aggregate_outputs
            .iter()
            .filter(|(input_name, _)| input_name == &raw_name)
            .cloned()
            .collect::<Vec<_>>();
        if outputs.len() > 1 {
            outputs
        } else if let Some(alias) = projection_info.output_aliases.get(&raw_name) {
            vec![(raw_name, alias.clone())]
        } else {
            outputs
        }
    } else if output_field_name_set.contains(&raw_name) {
        vec![(raw_name.clone(), raw_name)]
    } else {
        vec![]
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
                // Auto-created sink columns injected by ColumnMatcherRewriter
                // are not part of the original aggregate semantics and must
                // not prevent incremental aggregate rewrites.
                && name.as_str() != AUTO_CREATED_UPDATE_AT_TS_COL
                && name.as_str() != AUTO_CREATED_PLACEHOLDER_TS_COL
        })
        .cloned()
        .collect()
}

fn find_unsupported_group_key_projection_outputs(
    plan: &LogicalPlan,
    aggregate: &Aggregate,
    group_key_names: &[String],
) -> Vec<String> {
    let LogicalPlan::Projection(projection) = plan else {
        return vec![];
    };

    let group_key_names = group_key_names.iter().cloned().collect::<HashSet<_>>();
    let group_expr_names = aggregate
        .group_expr
        .iter()
        .filter_map(|expr| expr.name_for_alias().ok())
        .collect::<HashSet<_>>();
    projection
        .expr
        .iter()
        .filter_map(|expr| {
            let output_name = expr.qualified_name().1;
            if !group_key_names.contains(&output_name) {
                return None;
            }

            let source_name = match expr {
                Expr::Alias(alias) => alias.expr.name_for_alias().ok(),
                _ => expr.name_for_alias().ok(),
            };
            if source_name.is_some_and(|name| group_expr_names.contains(&name)) {
                None
            } else {
                Some(format!(
                    "unsupported group key output field is not a transparent group expression: {output_name}"
                ))
            }
        })
        .collect()
}

pub fn analyze_incremental_aggregate_plan(
    plan: &LogicalPlan,
) -> Result<Option<IncrementalAggregateAnalysis>, Error> {
    let group_key_names = find_group_key_names(plan)?;
    let aggregate = match extract_incremental_aggregate(plan) {
        Ok(Some(aggregate)) => aggregate,
        Ok(None) => return Ok(None),
        Err(reason) => {
            let projection_info = collect_output_projection_info(plan);
            let mut unsupported_exprs = projection_info
                .duplicate_output_names()
                .into_iter()
                .map(|name| format!("duplicate output field name: {name}"))
                .collect::<Vec<_>>();
            unsupported_exprs.push(reason);
            return Ok(Some(IncrementalAggregateAnalysis {
                group_key_names,
                merge_columns: vec![],
                literal_columns: vec![],
                output_field_names: projection_info.output_field_names,
                unsupported_exprs,
            }));
        }
    };
    let aggr_exprs = aggregate.aggr_expr.clone();
    let projection_info = collect_output_projection_info(plan);
    let output_field_name_set = projection_info.output_field_name_set();

    let mut merge_columns = Vec::with_capacity(aggr_exprs.len());
    let mut unsupported_exprs = projection_info
        .duplicate_output_names()
        .into_iter()
        .map(|name| format!("duplicate output field name: {name}"))
        .collect::<Vec<_>>();
    if has_grouping_set(plan) {
        unsupported_exprs.push(
            "unsupported GROUPING SETS/CUBE/ROLLUP in incremental aggregate rewrite".to_string(),
        );
    }
    if group_key_names.is_empty() {
        unsupported_exprs
            .push("unsupported global aggregate in incremental aggregate rewrite".to_string());
    }
    unsupported_exprs.extend(find_unsupported_group_key_projection_outputs(
        plan,
        aggregate,
        &group_key_names,
    ));
    for aggr_expr in aggr_exprs {
        let merge_op = match merge_op_for_aggregate_expr(&aggr_expr, aggregate.input.schema()) {
            Ok(merge_op) => merge_op,
            Err(reason) => {
                unsupported_exprs.push(reason);
                continue;
            }
        };
        let aggregate_outputs =
            resolve_aggregate_output_fields(&aggr_expr, &projection_info, &output_field_name_set);
        if aggregate_outputs.is_empty() {
            unsupported_exprs.push(aggr_expr.to_string());
            continue;
        }
        let Some((_, input_field_name)) = aggregate_outputs.first() else {
            continue;
        };
        // The old single-alias resolver selected the projected output name as
        // the delta field. Keep that exact field for the shared input and only
        // vary the final sink/output alias for repeated projections.
        let input_field_name = input_field_name.clone();
        for (_, output_field_name) in aggregate_outputs {
            merge_columns.push(IncrementalAggregateMergeColumn {
                input_field_name: input_field_name.clone(),
                output_field_name,
                merge_op,
            });
        }
    }
    if projection_info.has_top_level_projection {
        let output_positions = projection_info
            .output_field_names
            .iter()
            .enumerate()
            .map(|(position, name)| (name.as_str(), position))
            .collect::<HashMap<_, _>>();
        merge_columns.sort_by_key(|column| {
            output_positions
                .get(column.output_field_name.as_str())
                .copied()
                .unwrap_or(usize::MAX)
        });
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
/// sink_scan = SELECT * FROM <sink_table> [WHERE <sink_dirty_filter>]
/// sink  = SELECT ts, number FROM sink_scan AS __flow_sink
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
///
/// If `sink_dirty_filter` is provided, it is applied to the sink table scan
/// before projection, aliasing, and the left join. The predicate must reference
/// raw sink table columns structurally (unqualified), before the `__flow_sink`
/// alias exists.
pub async fn rewrite_incremental_aggregate_with_sink_merge(
    delta_plan: &LogicalPlan,
    analysis: &IncrementalAggregateAnalysis,
    engine: &QueryEngineRef,
    sink_table: TableRef,
    sink_table_name: &TableName,
    sink_dirty_filter: Option<Expr>,
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

    let state_merge = analysis
        .merge_columns
        .iter()
        .any(|column| matches!(column.merge_op, IncrementalAggregateMergeOp::AvgDeltaMerge));
    let mut selected_columns = analysis.group_key_names.clone();
    selected_columns.extend(
        analysis
            .merge_columns
            .iter()
            .map(|c| c.output_field_name.clone()),
    );
    let mut selected_column_names = HashSet::new();
    selected_columns.retain(|name| selected_column_names.insert(name.clone()));
    let mut delta_selected_columns = analysis.group_key_names.clone();
    delta_selected_columns.extend(
        analysis
            .merge_columns
            .iter()
            .map(|c| c.input_field_name.clone()),
    );
    delta_selected_columns.extend(analysis.literal_columns.iter().cloned());
    let mut delta_selected_column_names = HashSet::new();
    delta_selected_columns.retain(|name| delta_selected_column_names.insert(name.clone()));

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
    let sink_input = if let Some(predicate) = sink_dirty_filter {
        LogicalPlanBuilder::from(sink_scan)
            .filter(predicate)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to filter sink table scan for incremental sink merge".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build filtered sink plan for incremental sink merge"
                    .to_string(),
            })?
    } else {
        sink_scan
    };

    let sink_selected = LogicalPlanBuilder::from(sink_input)
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
    let mut group_exprs = Vec::new();
    let mut state_aggr_exprs = Vec::new();
    for output_field_name in &analysis.output_field_names {
        if group_key_names.contains(output_field_name)
            || literal_columns.contains(output_field_name)
        {
            let expr =
                qualified_col(delta_alias, output_field_name.clone()).alias(output_field_name);
            projection_exprs.push(expr.clone());
            group_exprs.push(expr);
        } else if let Some(merge_col) = merge_columns.get(output_field_name) {
            if matches!(
                merge_col.merge_op,
                IncrementalAggregateMergeOp::AvgDeltaMerge
            ) {
                state_aggr_exprs.push(build_state_delta_merge_expr(engine, merge_col)?);
            } else {
                let expr = build_left_join_merge_expr(delta_alias, sink_alias, merge_col)?;
                projection_exprs.push(expr.clone());
                group_exprs.push(expr);
            }
        } else {
            return InvalidQuerySnafu {
                reason: format!(
                    "UNSUPPORTED_INCREMENTAL_AGG: output field {output_field_name} is not covered by group keys, literals, or merge columns"
                ),
            }
            .fail();
        }
    }

    if state_merge {
        let aggregated = LogicalPlanBuilder::from(joined)
            .aggregate(group_exprs, state_aggr_exprs)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to aggregate state delta merge plan".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build state delta merge plan".to_string(),
            })?;
        let output_exprs = analysis
            .output_field_names
            .iter()
            .cloned()
            .map(unqualified_col)
            .collect::<Vec<_>>();
        LogicalPlanBuilder::from(aggregated)
            .project(output_exprs)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to project state delta merge plan".to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to finalize incremental aggregate sink merge plan".to_string(),
            })
    } else {
        LogicalPlanBuilder::from(joined)
            .project(projection_exprs)
            .with_context(|_| DatafusionSnafu {
                context: "Failed to build projection merge plan for incremental sink merge"
                    .to_string(),
            })?
            .build()
            .with_context(|_| DatafusionSnafu {
                context: "Failed to finalize incremental aggregate sink merge plan".to_string(),
            })
    }
}

fn build_state_delta_merge_expr(
    engine: &QueryEngineRef,
    merge_col: &IncrementalAggregateMergeColumn,
) -> Result<Expr, Error> {
    let Some(udaf) = engine
        .engine_state()
        .aggr_function("__avg_state_delta_merge")
        .or_else(|| {
            engine
                .engine_state()
                .session_state()
                .aggregate_functions()
                .get("__avg_state_delta_merge")
                .map(|udaf| udaf.as_ref().clone())
        })
    else {
        return InvalidQuerySnafu {
            reason: "Aggregate function __avg_state_delta_merge is not registered".to_string(),
        }
        .fail();
    };
    Ok(udaf
        .call(vec![
            qualified_col("__flow_delta", merge_col.input_field_name.clone()),
            qualified_col("__flow_sink", merge_col.output_field_name.clone()),
        ])
        .alias(merge_col.output_field_name.clone()))
}

fn build_left_join_merge_expr(
    delta_alias: &str,
    sink_alias: &str,
    merge_col: &IncrementalAggregateMergeColumn,
) -> Result<Expr, Error> {
    let left = qualified_col(delta_alias, merge_col.input_field_name.clone());
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
        IncrementalAggregateMergeOp::AvgDeltaMerge => {
            return InvalidQuerySnafu {
                reason: "state aggregate must be built with its delta UDAF".to_string(),
            }
            .fail();
        }
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
    let scheduled_time = query::options::parse_scheduled_time_datetime(&query_ctx.extensions())
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let stmts = ParserContext::create_with_dialect(
        sql,
        query_ctx.sql_dialect(),
        ParseOptions { scheduled_time },
    )
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
    gen_plan_with_matching_schema_and_values(
        sql,
        query_ctx,
        engine,
        sink_table_schema,
        primary_key_indices,
        allow_partial,
        None,
    )
    .await
}

pub(crate) async fn gen_plan_with_matching_schema_and_values(
    sql: &str,
    query_ctx: QueryContextRef,
    engine: QueryEngineRef,
    sink_table_schema: SchemaRef,
    primary_key_indices: &[usize],
    allow_partial: bool,
    ordinary_values: Option<&BTreeMap<String, ScalarValue>>,
) -> Result<LogicalPlan, Error> {
    let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), sql, false).await?;

    let mut add_auto_column = ColumnMatcherRewriter::new_with_values(
        sink_table_schema,
        primary_key_indices.to_vec(),
        allow_partial,
        ordinary_values.cloned().unwrap_or_default(),
    );
    let plan = plan
        .clone()
        .rewrite(&mut add_auto_column)
        .with_context(|_| DatafusionSnafu {
            context: "Failed to rewrite plan".to_string(),
        })?
        .data;
    Ok(plan)
}

pub fn df_plan_to_sql(plan: &LogicalPlan) -> Result<String, Error> {
    /// A dialect that forces identifiers to be quoted when they contain
    /// anything other than lowercase alphanumerics and underscores, or start
    /// with a digit.
    ///
    /// Unquoted identifiers are normalized to lowercase by the SQL parser, so
    /// uppercase letters need quoting to preserve case. Special characters
    /// (e.g. ':' in Prometheus-style table names like
    /// `kube_pod_cpu_cores:sum`, '.', '-', spaces) and digit-leading names
    /// (e.g. `123metrics`) would produce invalid SQL if left unquoted, so they
    /// are quoted as well. SQL keywords are intentionally not checked here:
    /// quoting every ALL_KEYWORDS member would also quote common column names
    /// like `number`; the unparse failure path has an InsertIntoPlan fallback.
    struct ForceQuoteIdentifiers;
    impl datafusion::sql::unparser::dialect::Dialect for ForceQuoteIdentifiers {
        fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
            let is_plain = !identifier.is_empty()
                && !identifier.starts_with(|c: char| c.is_ascii_digit())
                && identifier
                    .chars()
                    .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
            if is_plain { None } else { Some('"') }
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
    pub ordinary_values: BTreeMap<String, ScalarValue>,
}

impl ColumnMatcherRewriter {
    pub fn new(schema: SchemaRef, primary_key_indices: Vec<usize>, allow_partial: bool) -> Self {
        Self::new_with_values(schema, primary_key_indices, allow_partial, BTreeMap::new())
    }

    pub fn new_with_values(
        schema: SchemaRef,
        primary_key_indices: Vec<usize>,
        allow_partial: bool,
        ordinary_values: BTreeMap<String, ScalarValue>,
    ) -> Self {
        Self {
            schema,
            is_rewritten: false,
            primary_key_indices,
            allow_partial,
            ordinary_values,
        }
    }

    /// modify the exprs in place so that it matches the schema and some auto columns are added
    fn modify_project_exprs(
        &mut self,
        mut exprs: Vec<Expr>,
        input_schema: &DFSchema,
    ) -> DfResult<Vec<Expr>> {
        let original_exprs = exprs.clone();
        self.validate_ordinary_values(&original_exprs)?;
        let original_names = original_exprs
            .iter()
            .map(|expr| expr.qualified_name().1)
            .collect::<Vec<_>>();
        let duplicated_output_names = duplicate_names(&original_names);
        if !duplicated_output_names.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Flow output schema contains duplicate column(s) {:?}. {}",
                duplicated_output_names,
                format_flow_sink_schema_mismatch(&original_exprs, self.schema.as_ref())
            )));
        }

        if self.allow_partial {
            // Partial matching is intentionally name-based. Ordinary values are injected before
            // it so they follow the same direct partial path as the other supplied columns.
            for (idx, column) in self.schema.column_schemas().iter().enumerate() {
                if let Some(value) = self.ordinary_values.get(&column.name) {
                    exprs.insert(
                        idx.min(exprs.len()),
                        datafusion_expr::lit(value.clone()).alias(column.name.clone()),
                    );
                }
            }
            return self.modify_project_exprs_with_partial(exprs);
        }

        // Ordinary values are persistence-owned columns, not flow outputs. Remove them from the
        // effective sink sequence while deciding whether the existing auto-column rules apply.
        // This keeps those columns from hiding an auto-created update_at column that precedes them.
        let effective_sink_columns = self
            .schema
            .column_schemas()
            .iter()
            .enumerate()
            .filter(|(_, column)| !self.ordinary_values.contains_key(&column.name))
            .collect::<Vec<_>>();
        let query_col_cnt = exprs.len();
        let effective_sink_col_cnt = effective_sink_columns.len();
        debug!("query_col_cnt={query_col_cnt}, effective_sink_col_cnt={effective_sink_col_cnt}");

        let placeholder_ts_expr =
            datafusion::logical_expr::lit(ScalarValue::TimestampMillisecond(Some(0), None))
                .alias(AUTO_CREATED_PLACEHOLDER_TS_COL);

        if query_col_cnt == effective_sink_col_cnt {
            // still need to add aliases, see below
        } else if query_col_cnt + 1 == effective_sink_col_cnt {
            let (_, last_col_schema) = effective_sink_columns.last().unwrap();

            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index()
                    == Some(
                        self.schema
                            .column_index_by_name(&last_col_schema.name)
                            .unwrap(),
                    )
            {
                exprs.push(placeholder_ts_expr);
            } else if last_col_schema.data_type.is_timestamp() {
                exprs.push(datafusion::prelude::now().alias(&last_col_schema.name));
            } else {
                return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                    &original_exprs,
                    self.schema.as_ref(),
                )));
            }
        } else if query_col_cnt + 2 == effective_sink_col_cnt {
            let (_, last_col_schema) = effective_sink_columns.last().unwrap();
            let (_, second_last_col_schema) = effective_sink_columns
                .get(effective_sink_col_cnt - 2)
                .unwrap();
            if second_last_col_schema.data_type.is_timestamp() {
                exprs.push(datafusion::prelude::now().alias(&second_last_col_schema.name));
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect the second last column in the table to be timestamp column, found column {} with type {:?}",
                    second_last_col_schema.name, second_last_col_schema.data_type
                )));
            }

            if last_col_schema.name == AUTO_CREATED_PLACEHOLDER_TS_COL
                && self.schema.timestamp_index()
                    == Some(
                        self.schema
                            .column_index_by_name(&last_col_schema.name)
                            .unwrap(),
                    )
            {
                exprs.push(placeholder_ts_expr);
            } else {
                return Err(DataFusionError::Plan(format!(
                    "Expect timestamp column {}, found {:?}",
                    AUTO_CREATED_PLACEHOLDER_TS_COL, last_col_schema
                )));
            }
        } else {
            return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                &original_exprs,
                self.schema.as_ref(),
            )));
        }

        let exprs = self.match_extra_output_columns(
            exprs,
            input_schema,
            &original_exprs,
            &effective_sink_columns,
        )?;

        // Put persistence-owned values back at their physical sink positions only after matching
        // flow expressions against the effective sequence.
        let mut exprs = exprs;
        for (idx, column) in self.schema.column_schemas().iter().enumerate() {
            if let Some(value) = self.ordinary_values.get(&column.name) {
                exprs.insert(
                    idx.min(exprs.len()),
                    datafusion_expr::lit(value.clone()).alias(column.name.clone()),
                );
            }
        }
        self.order_by_sink_schema(exprs, &original_exprs)
    }

    fn order_by_sink_schema(
        &self,
        exprs: Vec<Expr>,
        original_exprs: &[Expr],
    ) -> DfResult<Vec<Expr>> {
        let mut by_name = exprs
            .into_iter()
            .map(|expr| (expr.qualified_name().1, expr))
            .collect::<HashMap<_, _>>();
        let mut ordered = Vec::with_capacity(self.schema.column_schemas().len());
        for column in self.schema.column_schemas() {
            if let Some(expr) = by_name.remove(&column.name) {
                ordered.push(expr);
            }
        }
        if !by_name.is_empty() || ordered.len() != self.schema.column_schemas().len() {
            return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                original_exprs,
                self.schema.as_ref(),
            )));
        }
        Ok(ordered)
    }

    fn validate_ordinary_values(&self, output_exprs: &[Expr]) -> DfResult<()> {
        let output_names = output_exprs
            .iter()
            .map(|expr| expr.qualified_name().1)
            .collect::<HashSet<_>>();
        for (name, value) in &self.ordinary_values {
            let Some(column) = self.schema.column_schema_by_name(name) else {
                return Err(DataFusionError::Plan(format!(
                    "Configured batching metadata column {name} does not exist in sink schema"
                )));
            };
            if output_names.contains(name) {
                return Err(DataFusionError::Plan(format!(
                    "Configured batching metadata column {name} collides with a flow output"
                )));
            }
            if value.data_type() != column.data_type.as_arrow_type() {
                return Err(DataFusionError::Plan(format!(
                    "Configured batching metadata column {name} has incompatible type"
                )));
            }
        }
        Ok(())
    }

    /// Match flow output columns whose names are not in the sink schema by the same position only.
    ///
    /// This keeps the legacy "omit output aliases and map by position" behavior, but only when the
    /// sink column at the same index is actually missing from the flow output. If the extra output
    /// would be aliased to a sink column that already exists elsewhere, report a schema mismatch
    /// instead of guessing another sink column by type.
    ///
    /// In particular, this intentionally rejects cross-position remaps like
    /// `record_time_window2 -> record_time_window`: they are easy to confuse with real schema
    /// mismatches and should be fixed by giving the flow output the sink column name explicitly.
    fn match_extra_output_columns(
        &self,
        mut exprs: Vec<Expr>,
        input_schema: &DFSchema,
        original_exprs: &[Expr],
        effective_sink_columns: &[(usize, &ColumnSchema)],
    ) -> DfResult<Vec<Expr>> {
        let mut output_names = exprs
            .iter()
            .map(|expr| expr.qualified_name().1)
            .collect::<Vec<_>>();
        let sink_names = effective_sink_columns
            .iter()
            .map(|(_, column)| column.name.as_str())
            .collect::<HashSet<_>>();
        let output_name_set = output_names.iter().cloned().collect::<BTreeSet<_>>();
        let extra_expr_indices = output_names
            .iter()
            .enumerate()
            .filter_map(|(idx, name)| (!sink_names.contains(name.as_str())).then_some(idx))
            .collect::<Vec<_>>();
        let missing_sink_indices = effective_sink_columns
            .iter()
            .enumerate()
            .filter_map(|(idx, (_, column))| {
                (!output_name_set.contains(&column.name)).then_some(idx)
            })
            .collect::<Vec<_>>();

        if extra_expr_indices.is_empty() && missing_sink_indices.is_empty() {
            return Ok(exprs);
        }

        if extra_expr_indices.len() != missing_sink_indices.len() {
            return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                original_exprs,
                self.schema.as_ref(),
            )));
        }

        let mut positional_matches = Vec::new();
        for expr_idx in extra_expr_indices {
            if !missing_sink_indices.contains(&expr_idx) {
                return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                    original_exprs,
                    self.schema.as_ref(),
                )));
            }

            let (_, target_col_schema) = effective_sink_columns[expr_idx];
            let expr_type =
                ConcreteDataType::from_arrow_type(&exprs[expr_idx].get_type(input_schema)?);
            if is_obviously_incompatible_positional_match(&expr_type, &target_col_schema.data_type)
            {
                return Err(DataFusionError::Plan(format!(
                    "Cannot match flow output column '{}' to sink column '{}' by position: incompatible data types, flow output type is {:?}, sink column type is {:?}. {}",
                    output_names[expr_idx],
                    target_col_schema.name,
                    expr_type,
                    target_col_schema.data_type,
                    format_flow_sink_schema_mismatch(original_exprs, self.schema.as_ref())
                )));
            }

            let target_name = target_col_schema.name.clone();
            positional_matches.push(format!(
                "{} -> {} (flow output type: {:?}, sink column type: {:?})",
                output_names[expr_idx], target_name, expr_type, target_col_schema.data_type
            ));
            exprs[expr_idx] = exprs[expr_idx].clone().alias(target_name.clone());
            output_names[expr_idx] = target_name;
        }

        if !positional_matches.is_empty() {
            debug!(
                "Matched flow output columns to sink columns by position: {:?}",
                positional_matches
            );
        }

        let duplicated_output_names = duplicate_names(&output_names);
        if !duplicated_output_names.is_empty() {
            return Err(DataFusionError::Plan(format!(
                "Flow output schema contains duplicate column(s) after schema matching {:?}. {}",
                duplicated_output_names,
                format_flow_sink_schema_mismatch(&exprs, self.schema.as_ref())
            )));
        }

        Ok(exprs)
    }

    fn modify_project_exprs_with_partial(&mut self, exprs: Vec<Expr>) -> DfResult<Vec<Expr>> {
        let table_col_cnt = self.schema.column_schemas().len();
        let query_col_cnt = exprs.len();

        if query_col_cnt > table_col_cnt {
            return Err(DataFusionError::Plan(format_flow_sink_schema_mismatch(
                &exprs,
                self.schema.as_ref(),
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
                "Column(s) {:?} required by sink table are missing from flow output when merge_mode=last_non_null. {}",
                missing,
                format_flow_sink_schema_mismatch(&exprs, self.schema.as_ref())
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
                "Flow output has extra column(s) {:?} not found in sink schema when merge_mode=last_non_null. {}",
                extra,
                format_flow_sink_schema_mismatch(&exprs, self.schema.as_ref())
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

fn is_obviously_incompatible_positional_match(
    expr_type: &ConcreteDataType,
    sink_type: &ConcreteDataType,
) -> bool {
    // This is a coarse type-family guard for legacy positional aliasing, not a strict type equality
    // check. For example, numeric width/sign differences are allowed here and left to downstream
    // coercion, and untyped NULL can be coerced to any target type. Clearly different families such
    // as timestamp vs string are rejected early.
    if expr_type.is_null() || expr_type == sink_type {
        return false;
    }

    expr_type.is_timestamp() != sink_type.is_timestamp()
        || expr_type.is_string() != sink_type.is_string()
        || expr_type.is_boolean() != sink_type.is_boolean()
        || expr_type.is_json() != sink_type.is_json()
        || expr_type.is_vector() != sink_type.is_vector()
}

fn duplicate_names(names: &[String]) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut duplicated = BTreeSet::new();
    for name in names {
        if !seen.insert(name.as_str()) {
            duplicated.insert(name.as_str());
        }
    }
    duplicated.into_iter().map(str::to_string).collect()
}

fn format_flow_sink_schema_mismatch(
    query_exprs: &[Expr],
    sink_schema: &datatypes::schema::Schema,
) -> String {
    let flow_output_columns = query_exprs
        .iter()
        .map(|expr| expr.qualified_name().1)
        .collect::<Vec<_>>();
    let sink_table_columns = sink_schema
        .column_schemas()
        .iter()
        .map(|col| col.name.clone())
        .collect::<Vec<_>>();

    let flow_output_set = flow_output_columns.iter().cloned().collect::<HashSet<_>>();
    let sink_table_set = sink_table_columns.iter().cloned().collect::<HashSet<_>>();

    let mut extra_flow_columns = flow_output_columns
        .iter()
        .filter(|name| !sink_table_set.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    extra_flow_columns.sort();
    extra_flow_columns.dedup();

    let mut missing_sink_columns = sink_table_columns
        .iter()
        .filter(|name| !flow_output_set.contains(*name))
        .cloned()
        .collect::<Vec<_>>();
    missing_sink_columns.sort();
    missing_sink_columns.dedup();

    format!(
        "Flow output schema does not match sink table schema: found {} flow output columns and {} sink table columns. flow output columns: {:?}, sink table columns: {:?}, extra flow columns not in sink: {:?}, missing sink columns from flow output: {:?}",
        flow_output_columns.len(),
        sink_table_columns.len(),
        flow_output_columns,
        sink_table_columns,
        extra_flow_columns,
        missing_sink_columns
    )
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
            let exprs = self.modify_project_exprs(exprs, project.input.schema())?;

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
            let exprs = self.modify_project_exprs(exprs, node.schema())?;
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
