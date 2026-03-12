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

use std::collections::HashSet;
use std::sync::Arc;

use common_error::ext::BoxedError;
use common_telemetry::debug;
use datafusion::config::ConfigOptions;
use datafusion::error::DataFusionError;
use datafusion::optimizer::analyzer::type_coercion::TypeCoercion;
use datafusion::optimizer::common_subexpr_eliminate::CommonSubexprEliminate;
use datafusion::optimizer::optimize_projections::OptimizeProjections;
use datafusion::optimizer::simplify_expressions::SimplifyExpressions;
use datafusion::optimizer::{Analyzer, AnalyzerRule, Optimizer, OptimizerContext};
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeVisitor};
use query::QueryEngine;
use query::optimizer::count_wildcard::CountWildcardToTimeIndexRule;
use query::parser::QueryLanguageParser;
use query::query_engine::DefaultSerializer;
use session::context::QueryContextRef;
use snafu::ResultExt;
/// note here we are using the `substrait_proto_df` crate from the `substrait` module and
/// rename it to `substrait_proto`
use substrait::DFLogicalSubstraitConvertor;

use crate::adapter::FlownodeContext;
use crate::error::{DatafusionSnafu, Error, ExternalSnafu, UnexpectedSnafu};
use crate::plan::TypedPlan;

// TODO(discord9): use `Analyzer` to manage rules if more `AnalyzerRule` is needed
pub async fn apply_df_optimizer(
    plan: datafusion_expr::LogicalPlan,
    query_ctx: &QueryContextRef,
) -> Result<datafusion_expr::LogicalPlan, Error> {
    let cfg = query_ctx.create_config_options();
    let analyzer = Analyzer::with_rules(vec![
        Arc::new(CountWildcardToTimeIndexRule),
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
        .plan(&stmt, query_ctx.clone())
        .await
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let opted_plan = apply_df_optimizer(plan, &query_ctx).await?;

    // TODO(discord9): add df optimization
    let sub_plan = DFLogicalSubstraitConvertor {}
        .to_sub_plan(&opted_plan, DefaultSerializer)
        .map_err(BoxedError::new)
        .context(ExternalSnafu)?;

    let flow_plan = TypedPlan::from_substrait_plan(ctx, &sub_plan).await?;

    Ok(flow_plan)
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
    if let datafusion_expr::LogicalPlan::Projection(proj) = &plan
        && let datafusion_expr::LogicalPlan::Aggregate(aggr) = proj.input.as_ref()
    {
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
                return Err(DataFusionError::Plan(format!(
                    "Expect {} expr in group by also exist in select list, but select list only contain {:?}",
                    expr.name_for_alias()?,
                    found_column_used.names_for_alias
                )));
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
