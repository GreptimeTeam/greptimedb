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

//! Datafusion optimizer for flow plan

#![warn(unused)]

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::functions_aggregate::count::count_udaf;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::utils::NamePreserver;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerContext};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor,
};
use datafusion_common::{Column, DFSchema, ScalarValue};
use datafusion_expr::utils::merge_schema;
use datafusion_expr::{
    BinaryExpr, ColumnarValue, Expr, Literal, Operator, Projection, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignature, Volatility,
};
use query::optimizer::count_wildcard::CountWildcardToTimeIndexRule;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use query::QueryEngine;
use snafu::ResultExt;
/// note here we are using the `substrait_proto_df` crate from the `substrait` module and
/// rename it to `substrait_proto`
use substrait::DFLogicalSubstraitConvertor;

use crate::adapter::FlownodeContext;
use crate::error::{DatafusionSnafu, Error, ExternalSnafu, UnexpectedSnafu};
use crate::expr::{TUMBLE_END, TUMBLE_START};
use crate::plan::TypedPlan;

// TODO(discord9): use `Analyzer` to manage rules if more `AnalyzerRule` is needed
pub async fn apply_df_optimizer(
    plan: datafusion_expr::LogicalPlan,
) -> Result<datafusion_expr::LogicalPlan, Error> {
    let cfg = ConfigOptions::new();
    let analyzer = Analyzer::with_rules(vec![
        Arc::new(CountWildcardToTimeIndexRule),
        Arc::new(AvgExpandRule),
        Arc::new(TumbleExpandRule),
        Arc::new(CheckGroupByRule::new()),
        Arc::new(TypeCoercion::new()),
    ]);
    let plan = analyzer
        .execute_and_check(plan, &cfg, |p, r| {
            debug!("After apply rule {}, get plan: \n{:?}", r.name(), p);
        })
        .context(DatafusionSnafu {
            context: "Fail to apply analyzer",
        })?;

    let ctx = OptimizerContext::new();
    let optimizer = Optimizer::with_rules(vec![
        Arc::new(OptimizeProjections::new()),
        Arc::new(CommonSubexprEliminate::new()),
        Arc::new(SimplifyExpressions::new()),
    ]);
    let plan = optimizer
        .optimize(plan, &ctx, |_, _| {})
        .context(DatafusionSnafu {
            context: "Fail to apply optimizer",
        })?;

    Ok(plan)
}

/// To reuse existing code for parse sql, the sql is first parsed into a datafusion logical plan,
/// then to a substrait plan, and finally to a flow plan.
pub async fn sql_to_flow_plan(
    ctx: &mut FlownodeContext,
    engine: &Arc<dyn QueryEngine>,
    sql: &str,
) -> Result<TypedPlan, Error> {
    let query_ctx = ctx.query_context.clone().ok_or_else(|| {
        UnexpectedSnafu {
            reason: "Query context is missing",
        }
        .build()
    })?;
    let stmt = QueryLanguageParser::parse_sql(sql, &query_ctx)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;
    let plan = engine
        .planner()
        .plan(&stmt, query_ctx)
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let opted_plan = apply_df_optimizer(plan).await?;

    // TODO(discord9): add df optimization
    let sub_plan = DFLogicalSubstraitConvertor {}
        .to_sub_plan(&opted_plan, DefaultSerializer)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let flow_plan = TypedPlan::from_substrait_plan(ctx, &sub_plan).await?;

    Ok(flow_plan)
}

#[derive(Debug)]
struct AvgExpandRule;

impl AnalyzerRule for AvgExpandRule {
    fn analyze(
        &self,
        plan: datafusion_expr::LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
        let transformed = plan
            .transform_up_with_subqueries(expand_avg_analyzer)?
            .data
            .transform_down_with_subqueries(put_aggr_to_proj_analyzer)?
            .data;
        Ok(transformed)
    }

    fn name(&self) -> &str {
        "avg_expand"
    }
}

/// lift aggr's composite aggr_expr to outer proj, and leave aggr only with simple direct aggr expr
/// i.e.
/// ```ignore
/// proj: avg(x)
/// -- aggr: [sum(x)/count(x) as avg(x)]
/// ```
/// becomes:
/// ```ignore
/// proj: sum(x)/count(x) as avg(x)
/// -- aggr: [sum(x), count(x)]
/// ```
fn put_aggr_to_proj_analyzer(
    plan: datafusion_expr::LogicalPlan,
) -> Result<Transformed<datafusion_expr::LogicalPlan>, DataFusionError> {
    if let datafusion_expr::LogicalPlan::Projection(proj) = &plan {
        if let datafusion_expr::LogicalPlan::Aggregate(aggr) = proj.input.as_ref() {
            let mut replace_old_proj_exprs = HashMap::new();
            let mut expanded_aggr_exprs = vec![];
            for aggr_expr in &aggr.aggr_expr {
                let mut is_composite = false;
                if let Expr::AggregateFunction(_) = &aggr_expr {
                    expanded_aggr_exprs.push(aggr_expr.clone());
                } else {
                    let old_name = aggr_expr.name_for_alias()?;
                    let new_proj_expr = aggr_expr
                        .clone()
                        .transform(|ch| {
                            if let Expr::AggregateFunction(_) = &ch {
                                is_composite = true;
                                expanded_aggr_exprs.push(ch.clone());
                                Ok(Transformed::yes(Expr::Column(Column::from_qualified_name(
                                    ch.name_for_alias()?,
                                ))))
                            } else {
                                Ok(Transformed::no(ch))
                            }
                        })?
                        .data;
                    replace_old_proj_exprs.insert(old_name, new_proj_expr);
                }
            }

            if expanded_aggr_exprs.len() > aggr.aggr_expr.len() {
                let mut aggr = aggr.clone();
                aggr.aggr_expr = expanded_aggr_exprs;
                let mut aggr_plan = datafusion_expr::LogicalPlan::Aggregate(aggr);
                // important to recompute schema after changing aggr_expr
                aggr_plan = aggr_plan.recompute_schema()?;

                // reconstruct proj with new proj_exprs
                let mut new_proj_exprs = proj.expr.clone();
                for proj_expr in new_proj_exprs.iter_mut() {
                    if let Some(new_proj_expr) =
                        replace_old_proj_exprs.get(&proj_expr.name_for_alias()?)
                    {
                        *proj_expr = new_proj_expr.clone();
                    }
                    *proj_expr = proj_expr
                        .clone()
                        .transform(|expr| {
                            if let Some(new_expr) =
                                replace_old_proj_exprs.get(&expr.name_for_alias()?)
                            {
                                Ok(Transformed::yes(new_expr.clone()))
                            } else {
                                Ok(Transformed::no(expr))
                            }
                        })?
                        .data;
                }
                let proj = datafusion_expr::LogicalPlan::Projection(Projection::try_new(
                    new_proj_exprs,
                    Arc::new(aggr_plan),
                )?);
                return Ok(Transformed::yes(proj));
            }
        }
    }
    Ok(Transformed::no(plan))
}

/// expand `avg(<expr>)` function into `cast(sum((<expr>) AS f64)/count((<expr>)`
fn expand_avg_analyzer(
    plan: datafusion_expr::LogicalPlan,
) -> Result<Transformed<datafusion_expr::LogicalPlan>, DataFusionError> {
    let mut schema = merge_schema(&plan.inputs());

    if let datafusion_expr::LogicalPlan::TableScan(ts) = &plan {
        let source_schema =
            DFSchema::try_from_qualified_schema(ts.table_name.clone(), &ts.source.schema())?;
        schema.merge(&source_schema);
    }

    let mut expr_rewrite = ExpandAvgRewriter::new(&schema);

    let name_preserver = NamePreserver::new(&plan);
    // apply coercion rewrite all expressions in the plan individually
    plan.map_expressions(|expr| {
        let original_name = name_preserver.save(&expr);
        Ok(expr
            .rewrite(&mut expr_rewrite)?
            .update_data(|expr| original_name.restore(expr)))
    })?
    .map_data(|plan| plan.recompute_schema())
}

/// rewrite `avg(<expr>)` function into `CASE WHEN count(<expr>) !=0 THEN  cast(sum((<expr>) AS avg_return_type)/count((<expr>) ELSE 0`
///
/// TODO(discord9): support avg return type decimal128
///
/// see impl details at https://github.com/apache/datafusion/blob/4ad4f90d86c57226a4e0fb1f79dfaaf0d404c273/datafusion/expr/src/type_coercion/aggregates.rs#L457-L462
pub(crate) struct ExpandAvgRewriter<'a> {
    /// schema of the plan
    #[allow(unused)]
    pub(crate) schema: &'a DFSchema,
}

impl<'a> ExpandAvgRewriter<'a> {
    fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }
}

impl TreeNodeRewriter for ExpandAvgRewriter<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>, DataFusionError> {
        if let Expr::AggregateFunction(aggr_func) = &expr {
            if aggr_func.func.name() == "avg" {
                let sum_expr = {
                    let mut tmp = aggr_func.clone();
                    tmp.func = sum_udaf();
                    Expr::AggregateFunction(tmp)
                };
                let sum_cast = {
                    let mut tmp = sum_expr.clone();
                    tmp = Expr::Cast(datafusion_expr::Cast {
                        expr: Box::new(tmp),
                        data_type: arrow_schema::DataType::Float64,
                    });
                    tmp
                };

                let count_expr = {
                    let mut tmp = aggr_func.clone();
                    tmp.func = count_udaf();

                    Expr::AggregateFunction(tmp)
                };
                let count_expr_ref =
                    Expr::Column(Column::from_qualified_name(count_expr.name_for_alias()?));

                let div =
                    BinaryExpr::new(Box::new(sum_cast), Operator::Divide, Box::new(count_expr));
                let div_expr = Box::new(Expr::BinaryExpr(div));

                let zero = Box::new(0.lit());
                let not_zero =
                    BinaryExpr::new(Box::new(count_expr_ref), Operator::NotEq, zero.clone());
                let not_zero = Box::new(Expr::BinaryExpr(not_zero));
                let null = Box::new(Expr::Literal(ScalarValue::Null, None));

                let case_when =
                    datafusion_expr::Case::new(None, vec![(not_zero, div_expr)], Some(null));
                let case_when_expr = Expr::Case(case_when);

                return Ok(Transformed::yes(case_when_expr));
            }
        }

        Ok(Transformed::no(expr))
    }
}

/// expand tumble in aggr expr to tumble_start and tumble_end with column name like `window_start`
#[derive(Debug)]
struct TumbleExpandRule;

impl AnalyzerRule for TumbleExpandRule {
    fn analyze(
        &self,
        plan: datafusion_expr::LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
        let transformed = plan
            .transform_up_with_subqueries(expand_tumble_analyzer)?
            .data;
        Ok(transformed)
    }

    fn name(&self) -> &str {
        "tumble_expand"
    }
}

/// expand `tumble` in aggr expr to `tumble_start` and `tumble_end`, also expand related alias and column ref
///
/// will add `tumble_start` and `tumble_end` to outer projection if not exist before
fn expand_tumble_analyzer(
    plan: datafusion_expr::LogicalPlan,
) -> Result<Transformed<datafusion_expr::LogicalPlan>, DataFusionError> {
    if let datafusion_expr::LogicalPlan::Projection(proj) = &plan {
        if let datafusion_expr::LogicalPlan::Aggregate(aggr) = proj.input.as_ref() {
            let mut new_group_expr = vec![];
            let mut alias_to_expand = HashMap::new();
            let mut encountered_tumble = false;
            for expr in aggr.group_expr.iter() {
                match expr {
                    datafusion_expr::Expr::ScalarFunction(func) if func.name() == "tumble" => {
                        encountered_tumble = true;

                        let tumble_start = TumbleExpand::new(TUMBLE_START);
                        let tumble_start = datafusion_expr::expr::ScalarFunction::new_udf(
                            Arc::new(tumble_start.into()),
                            func.args.clone(),
                        );
                        let tumble_start = datafusion_expr::Expr::ScalarFunction(tumble_start);
                        let start_col_name = tumble_start.name_for_alias()?;
                        new_group_expr.push(tumble_start);

                        let tumble_end = TumbleExpand::new(TUMBLE_END);
                        let tumble_end = datafusion_expr::expr::ScalarFunction::new_udf(
                            Arc::new(tumble_end.into()),
                            func.args.clone(),
                        );
                        let tumble_end = datafusion_expr::Expr::ScalarFunction(tumble_end);
                        let end_col_name = tumble_end.name_for_alias()?;
                        new_group_expr.push(tumble_end);

                        alias_to_expand
                            .insert(expr.name_for_alias()?, (start_col_name, end_col_name));
                    }
                    _ => new_group_expr.push(expr.clone()),
                }
            }
            if !encountered_tumble {
                return Ok(Transformed::no(plan));
            }
            let mut new_aggr = aggr.clone();
            new_aggr.group_expr = new_group_expr;
            let new_aggr = datafusion_expr::LogicalPlan::Aggregate(new_aggr).recompute_schema()?;
            // replace alias in projection if needed, and add new column ref if necessary
            let mut new_proj_expr = vec![];
            let mut have_expanded = false;

            for proj_expr in proj.expr.iter() {
                if let Some((start_col_name, end_col_name)) =
                    alias_to_expand.get(&proj_expr.name_for_alias()?)
                {
                    let start_col = Column::from_qualified_name(start_col_name);
                    let end_col = Column::from_qualified_name(end_col_name);
                    new_proj_expr.push(datafusion_expr::Expr::Column(start_col));
                    new_proj_expr.push(datafusion_expr::Expr::Column(end_col));
                    have_expanded = true;
                } else {
                    new_proj_expr.push(proj_expr.clone());
                }
            }

            // append to end of projection if not exist
            if !have_expanded {
                for (start_col_name, end_col_name) in alias_to_expand.values() {
                    let start_col = Column::from_qualified_name(start_col_name);
                    let end_col = Column::from_qualified_name(end_col_name);
                    new_proj_expr
                        .push(datafusion_expr::Expr::Column(start_col).alias("window_start"));
                    new_proj_expr.push(datafusion_expr::Expr::Column(end_col).alias("window_end"));
                }
            }

            let new_proj = datafusion_expr::LogicalPlan::Projection(Projection::try_new(
                new_proj_expr,
                Arc::new(new_aggr),
            )?);
            return Ok(Transformed::yes(new_proj));
        }
    }

    Ok(Transformed::no(plan))
}

/// This is a placeholder for tumble_start and tumble_end function, so that datafusion can
/// recognize them as scalar function
#[derive(Debug)]
pub struct TumbleExpand {
    signature: Signature,
    name: String,
}

impl TumbleExpand {
    pub fn new(name: &str) -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
            name: name.to_string(),
        }
    }
}

impl ScalarUDFImpl for TumbleExpand {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    /// elide the signature for now
    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(
        &self,
        arg_types: &[arrow_schema::DataType],
    ) -> datafusion_common::Result<Vec<arrow_schema::DataType>> {
        match (arg_types.first(), arg_types.get(1), arg_types.get(2)) {
            (Some(ts), Some(window), opt) => {
                use arrow_schema::DataType::*;
                if !matches!(ts, Date32 | Timestamp(_, _)) {
                    return Err(DataFusionError::Plan(
                        format!("Expect timestamp column as first arg for tumble_start, found {:?}", ts)
                    ));
                }
                if !matches!(window, Utf8 | Interval(_)) {
                    return Err(DataFusionError::Plan(
                        format!("Expect second arg for window size's type being interval for tumble_start, found {:?}", window),
                    ));
                }

                if let Some(start_time) = opt{
                    if !matches!(start_time,  Utf8 | Date32 | Timestamp(_, _)){
                        return Err(DataFusionError::Plan(
                            format!("Expect start_time to either be date, timestamp or string, found {:?}", start_time)
                        ));
                    }
                }

                Ok(arg_types.to_vec())
            }
            _ => Err(DataFusionError::Plan(
                "Expect tumble function have at least two arg(timestamp column and window size) and a third optional arg for starting time".to_string(),
            )),
        }
    }

    fn return_type(
        &self,
        arg_types: &[arrow_schema::DataType],
    ) -> Result<arrow_schema::DataType, DataFusionError> {
        arg_types.first().cloned().ok_or_else(|| {
            DataFusionError::Plan(
                "Expect tumble function have at least two arg(timestamp column and window size)"
                    .to_string(),
            )
        })
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        Err(DataFusionError::Plan(
            "This function should not be executed by datafusion".to_string(),
        ))
    }
}

/// This rule check all group by exprs, and make sure they are also in select clause in a aggr query
#[derive(Debug)]
struct CheckGroupByRule {}

impl CheckGroupByRule {
    pub fn new() -> Self {
        Self {}
    }
}

impl AnalyzerRule for CheckGroupByRule {
    fn analyze(
        &self,
        plan: datafusion_expr::LogicalPlan,
        _config: &ConfigOptions,
    ) -> datafusion_common::Result<datafusion_expr::LogicalPlan> {
        let transformed = plan
            .transform_up_with_subqueries(check_group_by_analyzer)?
            .data;
        Ok(transformed)
    }

    fn name(&self) -> &str {
        "check_groupby"
    }
}

/// make sure everything in group by's expr is in select
fn check_group_by_analyzer(
    plan: datafusion_expr::LogicalPlan,
) -> Result<Transformed<datafusion_expr::LogicalPlan>, DataFusionError> {
    if let datafusion_expr::LogicalPlan::Projection(proj) = &plan {
        if let datafusion_expr::LogicalPlan::Aggregate(aggr) = proj.input.as_ref() {
            let mut found_column_used = FindColumn::new();
            proj.expr
                .iter()
                .map(|i| i.visit(&mut found_column_used))
                .count();
            for expr in aggr.group_expr.iter() {
                if !found_column_used
                    .names_for_alias
                    .contains(&expr.name_for_alias()?)
                {
                    return Err(DataFusionError::Plan(format!("Expect {} expr in group by also exist in select list, but select list only contain {:?}",expr.name_for_alias()?, found_column_used.names_for_alias)));
                }
            }
        }
    }

    Ok(Transformed::no(plan))
}

/// Find all column names in a plan
#[derive(Debug, Default)]
struct FindColumn {
    names_for_alias: HashSet<String>,
}

impl FindColumn {
    fn new() -> Self {
        Default::default()
    }
}

impl TreeNodeVisitor<'_> for FindColumn {
    type Node = datafusion_expr::Expr;
    fn f_down(
        &mut self,
        node: &datafusion_expr::Expr,
    ) -> Result<TreeNodeRecursion, DataFusionError> {
        if let datafusion_expr::Expr::Column(_) = node {
            self.names_for_alias.insert(node.name_for_alias()?);
        }
        Ok(TreeNodeRecursion::Continue)
    }
}
