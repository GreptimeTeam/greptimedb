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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::array::{new_null_array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::common::alias::AliasGenerator;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter};
use datafusion::common::{Column, Result, ScalarValue};
use datafusion::logical_expr::expr_rewriter::create_col_from_scalar_expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{
    expr, EmptyRelation, Expr, JoinType, LogicalPlan, LogicalPlanBuilder, Subquery,
};
use datafusion::optimizer::decorrelate::{PullUpCorrelatedExpr, UN_MATCHED_ROW_INDICATOR};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::TransformedResult;
use datafusion_common::{internal_err, plan_err, DFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr_rewriter::replace_col;
use datafusion_expr::ColumnarValue;
use datafusion_optimizer::analyzer::type_coercion::TypeCoercionRewriter;
use datafusion_physical_expr::create_physical_expr;

/// Optimizer rule for rewriting scalar subquery filters to JOINs
#[derive(Default, Debug)]
pub struct ScalarSubqueryToJoin {}

impl ScalarSubqueryToJoin {
    /// Creates a new instance of the optimizer rule
    pub fn new() -> Self {
        Self::default()
    }

    /// Analyzes and transforms logical plans by rewriting scalar subqueries to JOINs
    pub fn analyze(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        match plan {
            LogicalPlan::Filter(filter) => self.optimize_filter(filter, config),
            LogicalPlan::Projection(projection) => self.optimize_projection(projection, config),
            _ => Ok(Transformed::no(plan)),
        }
    }

    /// Optimizes filter expressions containing scalar subqueries
    fn optimize_filter(
        &self,
        filter: datafusion_expr::Filter,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Skip optimization if no scalar subqueries are present
        if !contains_scalar_subquery(&filter.predicate) {
            return Ok(Transformed::no(LogicalPlan::Filter(filter)));
        }

        let (subqueries, mut rewrite_expr) =
            self.extract_subquery_exprs(&filter.predicate, config.alias_generator())?;

        if subqueries.is_empty() {
            return internal_err!("Expected subqueries not found in filter");
        }

        // Convert each subquery to a LEFT JOIN
        let mut cur_input = filter.input.as_ref().clone();
        for (subquery, alias) in subqueries {
            if let Some((optimized_subquery, expr_check_map)) =
                build_join(&subquery, &cur_input, &alias)?
            {
                // Rewrite expressions using the computed mapping
                if !expr_check_map.is_empty() {
                    rewrite_expr = rewrite_expr
                        .transform_up(|expr| {
                            expr.try_as_col()
                                .and_then(|col| expr_check_map.get(&col.name))
                                .map_or_else(
                                    || Ok(Transformed::no(expr)),
                                    |map_expr| Ok(Transformed::yes(map_expr.clone())),
                                )
                        })
                        .data()?;
                }
                cur_input = optimized_subquery;
            } else {
                // Bail if we can't handle all subqueries
                return Ok(Transformed::no(LogicalPlan::Filter(filter)));
            }
        }

        let new_plan = LogicalPlanBuilder::from(cur_input)
            .filter(rewrite_expr)?
            .build()?;
        Ok(Transformed::yes(new_plan))
    }

    /// Optimizes projection expressions containing scalar subqueries
    fn optimize_projection(
        &self,
        projection: datafusion_expr::Projection,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        // Skip optimization if no scalar subqueries are present
        if !projection.expr.iter().any(contains_scalar_subquery) {
            return Ok(Transformed::no(LogicalPlan::Projection(projection)));
        }

        let mut all_subqueries = vec![];
        let mut expr_to_rewrite_expr_map = HashMap::new();
        let mut subquery_to_expr_map = HashMap::new();

        // Extract subqueries from each projection expression
        for expr in projection.expr.iter() {
            let (subqueries, rewrite_exprs) =
                self.extract_subquery_exprs(expr, config.alias_generator())?;
            for (subquery, _) in &subqueries {
                subquery_to_expr_map.insert(subquery.clone(), expr.clone());
            }
            all_subqueries.extend(subqueries);
            expr_to_rewrite_expr_map.insert(expr, rewrite_exprs);
        }

        if all_subqueries.is_empty() {
            return internal_err!("Expected subqueries not found in projection");
        }

        // Convert each subquery to a LEFT JOIN
        let mut cur_input = projection.input.as_ref().clone();
        for (subquery, alias) in all_subqueries {
            if let Some((optimized_subquery, expr_check_map)) =
                build_join(&subquery, &cur_input, &alias)?
            {
                cur_input = optimized_subquery;
                if !expr_check_map.is_empty() {
                    if let Some(expr) = subquery_to_expr_map.get(&subquery) {
                        if let Some(rewrite_expr) = expr_to_rewrite_expr_map.get(expr) {
                            let new_expr = rewrite_expr
                                .clone()
                                .transform_up(|expr| {
                                    expr.try_as_col()
                                        .and_then(|col| expr_check_map.get(&col.name))
                                        .map_or_else(
                                            || Ok(Transformed::no(expr)),
                                            |map_expr| Ok(Transformed::yes(map_expr.clone())),
                                        )
                                })
                                .data()?;
                            expr_to_rewrite_expr_map.insert(expr, new_expr);
                        }
                    }
                }
            } else {
                // Bail if we can't handle all subqueries
                return Ok(Transformed::no(LogicalPlan::Projection(projection)));
            }
        }

        // Build new projection expressions
        let proj_exprs = projection
            .expr
            .iter()
            .map(|expr| {
                let old_expr_name = expr.schema_name().to_string();
                let new_expr = expr_to_rewrite_expr_map.get(expr).unwrap();
                let new_expr_name = new_expr.schema_name().to_string();
                if new_expr_name != old_expr_name {
                    new_expr.clone().alias(old_expr_name)
                } else {
                    new_expr.clone()
                }
            })
            .collect::<Vec<_>>();

        let new_plan = LogicalPlanBuilder::from(cur_input)
            .project(proj_exprs)?
            .build()?;
        Ok(Transformed::yes(new_plan))
    }

    /// Extracts scalar subqueries from an expression
    fn extract_subquery_exprs(
        &self,
        predicate: &Expr,
        alias_gen: &Arc<AliasGenerator>,
    ) -> Result<(Vec<(Subquery, String)>, Expr)> {
        let mut extract = ExtractScalarSubQuery {
            sub_query_info: vec![],
            alias_gen,
        };
        predicate
            .clone()
            .rewrite(&mut extract)
            .data()
            .map(|new_expr| (extract.sub_query_info, new_expr))
    }
}

impl OptimizerRule for ScalarSubqueryToJoin {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        self.analyze(plan, config)
    }

    fn name(&self) -> &str {
        "scalar_subquery_to_join"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Extracts scalar subquery information from expressions
struct ExtractScalarSubQuery<'a> {
    sub_query_info: Vec<(Subquery, String)>,
    alias_gen: &'a Arc<AliasGenerator>,
}

impl TreeNodeRewriter for ExtractScalarSubQuery<'_> {
    type Node = Expr;

    fn f_down(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        match expr {
            Expr::ScalarSubquery(subquery) => {
                let subqry_alias = self.alias_gen.next("__scalar_sq");
                self.sub_query_info
                    .push((subquery.clone(), subqry_alias.clone()));
                let scalar_expr = subquery
                    .subquery
                    .head_output_expr()?
                    .map_or(plan_err!("single expression required."), Ok)?;
                Ok(Transformed::new(
                    Expr::Column(create_col_from_scalar_expr(&scalar_expr, subqry_alias)?),
                    true,
                    TreeNodeRecursion::Jump,
                ))
            }
            _ => Ok(Transformed::no(expr)),
        }
    }
}

/// Builds JOIN operations to optimize scalar subqueries
fn build_join(
    subquery: &Subquery,
    filter_input: &LogicalPlan,
    subquery_alias: &str,
) -> Result<Option<(LogicalPlan, HashMap<String, Expr>)>> {
    let subquery_plan = subquery.subquery.as_ref();
    let mut pull_up = PullUpCorrelatedExpr::new().with_need_handle_count_bug(true);
    let new_plan = subquery_plan.clone().rewrite(&mut pull_up).data()?;
    if !pull_up.can_pull_up {
        return Ok(None);
    }

    let collected_count_expr_map = pull_up.collected_count_expr_map.get(&new_plan).cloned();
    let sub_query_alias = LogicalPlanBuilder::from(new_plan)
        .alias(subquery_alias.to_string())?
        .build()?;

    let mut all_correlated_cols = BTreeSet::new();
    pull_up
        .correlated_subquery_cols_map
        .values()
        .for_each(|cols| all_correlated_cols.extend(cols.clone()));

    // Alias join filter columns
    let join_filter_opt = conjunction(pull_up.join_filters).map_or(Ok(None), |filter| {
        replace_qualified_name(filter, &all_correlated_cols, subquery_alias).map(Some)
    })?;

    // Join subquery into main plan
    let new_plan = if join_filter_opt.is_none() {
        match filter_input {
            LogicalPlan::EmptyRelation(EmptyRelation {
                produce_one_row: true,
                schema: _,
            }) => sub_query_alias,
            _ => {
                // For uncorrelated subqueries: group to 1 row and LEFT JOIN
                LogicalPlanBuilder::from(filter_input.clone())
                    .join_on(
                        sub_query_alias,
                        JoinType::Left,
                        vec![Expr::Literal(ScalarValue::Boolean(Some(true)))],
                    )?
                    .build()?
            }
        }
    } else {
        // For correlated subqueries: LEFT JOIN preserving row count
        LogicalPlanBuilder::from(filter_input.clone())
            .join_on(sub_query_alias, JoinType::Left, join_filter_opt)?
            .build()?
    };

    let mut computation_project_expr = HashMap::new();
    if let Some(expr_map) = collected_count_expr_map {
        for (name, result) in expr_map {
            // Skip expressions that always return NULL
            if evaluates_to_null(result.clone(), result.column_refs())? {
                continue;
            }

            // Build computation expression
            let computer_expr = if let Some(filter) = &pull_up.pull_up_having_expr {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![
                        (
                            Box::new(Expr::IsNull(Box::new(Expr::Column(
                                Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                            )))),
                            Box::new(result),
                        ),
                        (
                            Box::new(Expr::Not(Box::new(filter.clone()))),
                            Box::new(Expr::Literal(ScalarValue::Null)),
                        ),
                    ],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            } else {
                Expr::Case(expr::Case {
                    expr: None,
                    when_then_expr: vec![(
                        Box::new(Expr::IsNull(Box::new(Expr::Column(
                            Column::new_unqualified(UN_MATCHED_ROW_INDICATOR),
                        )))),
                        Box::new(result),
                    )],
                    else_expr: Some(Box::new(Expr::Column(Column::new_unqualified(
                        name.clone(),
                    )))),
                })
            };

            // Apply type coercion
            let mut expr_rewrite = TypeCoercionRewriter::new(&*new_plan.schema().clone());
            computation_project_expr.insert(name, computer_expr.rewrite(&mut expr_rewrite).data()?);
        }
    }

    Ok(Some((new_plan, computation_project_expr)))
}

/// Checks if an expression contains scalar subqueries
fn contains_scalar_subquery(expr: &Expr) -> bool {
    expr.exists(|expr| Ok(matches!(expr, Expr::ScalarSubquery(_))))
        .expect("Inner is always Ok")
}

fn replace_qualified_name(
    expr: Expr,
    cols: &BTreeSet<Column>,
    subquery_alias: &str,
) -> Result<Expr> {
    let alias_cols: Vec<Column> = cols
        .iter()
        .map(|col| Column::new(Some(subquery_alias), &col.name))
        .collect();
    let replace_map: HashMap<&Column, &Column> = cols.iter().zip(alias_cols.iter()).collect();

    replace_col(expr, &replace_map)
}

/// Determines if an expression will always evaluate to null.
/// `c0 + 8` return true
/// `c0 IS NULL` return false
/// `CASE WHEN c0 > 1 then 0 else 1` return false
pub fn evaluates_to_null<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<bool> {
    if matches!(predicate, Expr::Column(_)) {
        return Ok(true);
    }

    Ok(
        match evaluate_expr_with_null_column(predicate, null_columns)? {
            ColumnarValue::Array(_) => false,
            ColumnarValue::Scalar(scalar) => scalar.is_null(),
        },
    )
}

fn evaluate_expr_with_null_column<'a>(
    predicate: Expr,
    null_columns: impl IntoIterator<Item = &'a Column>,
) -> Result<ColumnarValue> {
    static DUMMY_COL_NAME: &str = "?";
    let schema = Schema::new(vec![Field::new(DUMMY_COL_NAME, DataType::Null, true)]);
    let input_schema = DFSchema::try_from(schema.clone())?;
    let column = new_null_array(&DataType::Null, 1);
    let input_batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![column])?;
    let execution_props = ExecutionProps::default();
    let null_column = Column::from_name(DUMMY_COL_NAME);

    let join_cols_to_replace = null_columns
        .into_iter()
        .map(|column| (column, &null_column))
        .collect::<HashMap<_, _>>();

    let replaced_predicate = replace_col(predicate, &join_cols_to_replace)?;
    let coerced_predicate = coerce(replaced_predicate, &input_schema)?;
    create_physical_expr(&coerced_predicate, &input_schema, &execution_props)?
        .evaluate(&input_batch)
}

fn coerce(expr: Expr, schema: &DFSchema) -> Result<Expr> {
    let mut expr_rewrite = TypeCoercionRewriter::new(&schema);
    expr.rewrite(&mut expr_rewrite).data()
}
