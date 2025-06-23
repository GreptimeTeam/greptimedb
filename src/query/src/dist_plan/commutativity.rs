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

use std::collections::HashSet;
use std::sync::Arc;

use common_function::aggrs::approximate::hll::{HllState, HLL_MERGE_NAME, HLL_NAME};
use common_function::aggrs::approximate::uddsketch::{
    UddSketchState, UDDSKETCH_MERGE_NAME, UDDSKETCH_STATE_NAME,
};
use common_telemetry::debug;
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion_common::Column;
use datafusion_expr::{Expr, LogicalPlan, Projection, UserDefinedLogicalNode};
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

use crate::dist_plan::merge_sort::{merge_sort_transformer, MergeSortLogicalPlan};
use crate::dist_plan::MergeScanLogicalPlan;

/// generate the upper aggregation plan that will execute on the frontend.
/// Basically a logical plan resembling the following:
/// Projection:
///     Aggregate:
///
/// from Aggregate
///
/// The upper Projection exists sole to make sure parent plan can recognize the output
/// of the upper aggregation plan.
pub fn step_aggr_to_upper_aggr(
    aggr_plan: &LogicalPlan,
) -> datafusion_common::Result<[LogicalPlan; 2]> {
    let LogicalPlan::Aggregate(input_aggr) = aggr_plan else {
        return Err(datafusion_common::DataFusionError::Plan(
            "step_aggr_to_upper_aggr only accepts Aggregate plan".to_string(),
        ));
    };
    if !is_all_aggr_exprs_steppable(&input_aggr.aggr_expr) {
        return Err(datafusion_common::DataFusionError::NotImplemented(
            "Some aggregate expressions are not steppable".to_string(),
        ));
    }
    let mut upper_aggr_expr = vec![];
    for aggr_expr in &input_aggr.aggr_expr {
        let Some(aggr_func) = get_aggr_func(aggr_expr) else {
            return Err(datafusion_common::DataFusionError::NotImplemented(
                "Aggregate function not found".to_string(),
            ));
        };
        let col_name = aggr_expr.qualified_name();
        let input_column = Expr::Column(datafusion_common::Column::new(col_name.0, col_name.1));
        let upper_func = match aggr_func.func.name() {
            "sum" | "min" | "max" | "last_value" | "first_value" => {
                // aggr_calc(aggr_merge(input_column))) as col_name
                let mut new_aggr_func = aggr_func.clone();
                new_aggr_func.args = vec![input_column.clone()];
                new_aggr_func
            }
            "count" => {
                // sum(input_column) as col_name
                let mut new_aggr_func = aggr_func.clone();
                new_aggr_func.func = sum_udaf();
                new_aggr_func.args = vec![input_column.clone()];
                new_aggr_func
            }
            UDDSKETCH_STATE_NAME | UDDSKETCH_MERGE_NAME => {
                // udd_merge(bucket_size, error_rate input_column) as col_name
                let mut new_aggr_func = aggr_func.clone();
                new_aggr_func.func = Arc::new(UddSketchState::merge_udf_impl());
                new_aggr_func.args[2] = input_column.clone();
                new_aggr_func
            }
            HLL_NAME | HLL_MERGE_NAME => {
                // hll_merge(input_column) as col_name
                let mut new_aggr_func = aggr_func.clone();
                new_aggr_func.func = Arc::new(HllState::merge_udf_impl());
                new_aggr_func.args = vec![input_column.clone()];
                new_aggr_func
            }
            _ => {
                return Err(datafusion_common::DataFusionError::NotImplemented(format!(
                    "Aggregate function {} is not supported for Step aggregation",
                    aggr_func.func.name()
                )))
            }
        };

        // deal with nested alias case
        let mut new_aggr_expr = aggr_expr.clone();
        {
            let new_aggr_func = get_aggr_func_mut(&mut new_aggr_expr).unwrap();
            *new_aggr_func = upper_func;
        }

        upper_aggr_expr.push(new_aggr_expr);
    }
    let mut new_aggr = input_aggr.clone();
    // use lower aggregate plan as input, this will be replace by merge scan plan later
    new_aggr.input = Arc::new(LogicalPlan::Aggregate(input_aggr.clone()));

    new_aggr.aggr_expr = upper_aggr_expr;

    // group by expr also need to be all ref by column to avoid duplicated computing
    let mut new_group_expr = new_aggr.group_expr.clone();
    for expr in &mut new_group_expr {
        if let Expr::Column(_) = expr {
            // already a column, no need to change
            continue;
        }
        let col_name = expr.qualified_name();
        let input_column = Expr::Column(datafusion_common::Column::new(col_name.0, col_name.1));
        *expr = input_column;
    }
    new_aggr.group_expr = new_group_expr.clone();

    let mut new_projection_exprs = new_group_expr;
    // the upper aggr expr need to be aliased to the input aggr expr's name,
    // so that the parent plan can recognize it.
    for (lower_aggr_expr, upper_aggr_expr) in
        input_aggr.aggr_expr.iter().zip(new_aggr.aggr_expr.iter())
    {
        let lower_col_name = lower_aggr_expr.qualified_name();
        let (table, col_name) = upper_aggr_expr.qualified_name();
        let aggr_out_column = Column::new(table, col_name);
        let aliased_output_aggr_expr =
            Expr::Column(aggr_out_column).alias_qualified(lower_col_name.0, lower_col_name.1);
        new_projection_exprs.push(aliased_output_aggr_expr);
    }
    let upper_aggr_plan = LogicalPlan::Aggregate(new_aggr);
    debug!("Before recompute schema: {upper_aggr_plan:?}");
    let upper_aggr_plan = upper_aggr_plan.recompute_schema()?;
    debug!("After recompute schema: {upper_aggr_plan:?}");
    // create a projection on top of the new aggregate plan
    let new_projection =
        Projection::try_new(new_projection_exprs, Arc::new(upper_aggr_plan.clone()))?;
    let projection = LogicalPlan::Projection(new_projection);
    // return the new logical plan
    Ok([projection, upper_aggr_plan])
}

/// Check if the given aggregate expression is steppable.
/// As in if it can be split into multiple steps:
/// i.e. on datanode first call `state(input)` then
/// on frontend call `calc(merge(state))` to get the final result.
pub fn is_all_aggr_exprs_steppable(aggr_exprs: &[Expr]) -> bool {
    let step_action = HashSet::from([
        "sum",
        "count",
        "min",
        "max",
        "first_value",
        "last_value",
        UDDSKETCH_STATE_NAME,
        UDDSKETCH_MERGE_NAME,
        HLL_NAME,
        HLL_MERGE_NAME,
    ]);
    aggr_exprs.iter().all(|expr| {
        if let Some(aggr_func) = get_aggr_func(expr) {
            if aggr_func.distinct {
                // Distinct aggregate functions are not steppable(yet).
                return false;
            }
            step_action.contains(aggr_func.func.name())
        } else {
            false
        }
    })
}

pub fn get_aggr_func(expr: &Expr) -> Option<&datafusion_expr::expr::AggregateFunction> {
    let mut expr_ref = expr;
    while let Expr::Alias(alias) = expr_ref {
        expr_ref = &alias.expr;
    }
    if let Expr::AggregateFunction(aggr_func) = expr_ref {
        Some(aggr_func)
    } else {
        None
    }
}

pub fn get_aggr_func_mut(expr: &mut Expr) -> Option<&mut datafusion_expr::expr::AggregateFunction> {
    let mut expr_ref = expr;
    while let Expr::Alias(alias) = expr_ref {
        expr_ref = &mut alias.expr;
    }
    if let Expr::AggregateFunction(aggr_func) = expr_ref {
        Some(aggr_func)
    } else {
        None
    }
}

#[allow(dead_code)]
pub enum Commutativity {
    Commutative,
    PartialCommutative,
    ConditionalCommutative(Option<Transformer>),
    TransformedCommutative {
        /// Return plans from parent to child order
        transformer: Option<StageTransformer>,
    },
    NonCommutative,
    Unimplemented,
    /// For unrelated plans like DDL
    Unsupported,
}

pub struct Categorizer {}

impl Categorizer {
    pub fn check_plan(plan: &LogicalPlan, partition_cols: Option<Vec<String>>) -> Commutativity {
        let partition_cols = partition_cols.unwrap_or_default();

        match plan {
            LogicalPlan::Projection(proj) => {
                for expr in &proj.expr {
                    let commutativity = Self::check_expr(expr);
                    if !matches!(commutativity, Commutativity::Commutative) {
                        return commutativity;
                    }
                }
                Commutativity::Commutative
            }
            // TODO(ruihang): Change this to Commutative once Like is supported in substrait
            LogicalPlan::Filter(filter) => Self::check_expr(&filter.predicate),
            LogicalPlan::Window(_) => Commutativity::Unimplemented,
            LogicalPlan::Aggregate(aggr) => {
                let is_all_steppable = is_all_aggr_exprs_steppable(&aggr.aggr_expr);
                let matches_partition = Self::check_partition(&aggr.group_expr, &partition_cols);
                if !matches_partition && is_all_steppable {
                    debug!("Plan is steppable: {plan}");
                    return Commutativity::TransformedCommutative {
                        transformer: Some(Arc::new(|plan: &LogicalPlan| {
                            debug!("Before Step optimize: {plan}");
                            let ret = step_aggr_to_upper_aggr(plan);
                            debug!("After Step Optimize: {ret:?}");
                            ret.ok().map(|s| TransformerAction {
                                extra_parent_plans: s.to_vec(),
                                new_child_plan: None,
                            })
                        })),
                    };
                }
                if !matches_partition {
                    return Commutativity::NonCommutative;
                }
                for expr in &aggr.aggr_expr {
                    let commutativity = Self::check_expr(expr);
                    if !matches!(commutativity, Commutativity::Commutative) {
                        return commutativity;
                    }
                }
                Commutativity::Commutative
            }
            LogicalPlan::Sort(_) => {
                if partition_cols.is_empty() {
                    return Commutativity::Commutative;
                }

                // sort plan needs to consider column priority
                // Change Sort to MergeSort which assumes the input streams are already sorted hence can be more efficient
                // We should ensure the number of partition is not smaller than the number of region at present. Otherwise this would result in incorrect output.
                Commutativity::ConditionalCommutative(Some(Arc::new(merge_sort_transformer)))
            }
            LogicalPlan::Join(_) => Commutativity::NonCommutative,
            LogicalPlan::Repartition(_) => {
                // unsupported? or non-commutative
                Commutativity::Unimplemented
            }
            LogicalPlan::Union(_) => Commutativity::Unimplemented,
            LogicalPlan::TableScan(_) => Commutativity::Commutative,
            LogicalPlan::EmptyRelation(_) => Commutativity::NonCommutative,
            LogicalPlan::Subquery(_) => Commutativity::Unimplemented,
            LogicalPlan::SubqueryAlias(_) => Commutativity::Unimplemented,
            LogicalPlan::Limit(limit) => {
                // Only execute `fetch` on remote nodes.
                // wait for https://github.com/apache/arrow-datafusion/pull/7669
                if partition_cols.is_empty() && limit.fetch.is_some() {
                    Commutativity::Commutative
                } else if limit.skip.is_none() && limit.fetch.is_some() {
                    Commutativity::PartialCommutative
                } else {
                    Commutativity::Unimplemented
                }
            }
            LogicalPlan::Extension(extension) => {
                Self::check_extension_plan(extension.node.as_ref() as _, &partition_cols)
            }
            LogicalPlan::Distinct(_) => {
                if partition_cols.is_empty() {
                    Commutativity::Commutative
                } else {
                    Commutativity::Unimplemented
                }
            }
            LogicalPlan::Unnest(_) => Commutativity::Commutative,
            LogicalPlan::Statement(_) => Commutativity::Unsupported,
            LogicalPlan::Values(_) => Commutativity::Unsupported,
            LogicalPlan::Explain(_) => Commutativity::Unsupported,
            LogicalPlan::Analyze(_) => Commutativity::Unsupported,
            LogicalPlan::DescribeTable(_) => Commutativity::Unsupported,
            LogicalPlan::Dml(_) => Commutativity::Unsupported,
            LogicalPlan::Ddl(_) => Commutativity::Unsupported,
            LogicalPlan::Copy(_) => Commutativity::Unsupported,
            LogicalPlan::RecursiveQuery(_) => Commutativity::Unsupported,
        }
    }

    pub fn check_extension_plan(
        plan: &dyn UserDefinedLogicalNode,
        partition_cols: &[String],
    ) -> Commutativity {
        match plan.name() {
            name if name == SeriesDivide::name() => {
                let series_divide = plan.as_any().downcast_ref::<SeriesDivide>().unwrap();
                let tags = series_divide.tags().iter().collect::<HashSet<_>>();
                for partition_col in partition_cols {
                    if !tags.contains(partition_col) {
                        return Commutativity::NonCommutative;
                    }
                }
                Commutativity::Commutative
            }
            name if name == SeriesNormalize::name()
                || name == InstantManipulate::name()
                || name == RangeManipulate::name() =>
            {
                // They should always follows Series Divide.
                // Either all commutative or all non-commutative (which will be blocked by SeriesDivide).
                Commutativity::Commutative
            }
            name if name == EmptyMetric::name()
                || name == MergeScanLogicalPlan::name()
                || name == MergeSortLogicalPlan::name() =>
            {
                Commutativity::Unimplemented
            }
            _ => Commutativity::Unsupported,
        }
    }

    pub fn check_expr(expr: &Expr) -> Commutativity {
        match expr {
            Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_)
            | Expr::BinaryExpr(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::Negative(_)
            | Expr::Between(_)
            | Expr::Exists(_)
            | Expr::InList(_)
            | Expr::Case(_) => Commutativity::Commutative,
            Expr::ScalarFunction(_udf) => Commutativity::Commutative,
            Expr::AggregateFunction(_udaf) => Commutativity::Commutative,

            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::WindowFunction(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. } => Commutativity::Unimplemented,

            Expr::Alias(alias) => Self::check_expr(&alias.expr),

            Expr::Unnest(_)
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _) => Commutativity::Unimplemented,
        }
    }

    /// Return true if the given expr and partition cols satisfied the rule.
    /// In this case the plan can be treated as fully commutative.
    fn check_partition(exprs: &[Expr], partition_cols: &[String]) -> bool {
        let mut ref_cols = HashSet::new();
        for expr in exprs {
            expr.add_column_refs(&mut ref_cols);
        }
        let ref_cols = ref_cols
            .into_iter()
            .map(|c| c.name.clone())
            .collect::<HashSet<_>>();
        for col in partition_cols {
            if !ref_cols.contains(col) {
                return false;
            }
        }

        true
    }
}

pub type Transformer = Arc<dyn Fn(&LogicalPlan) -> Option<LogicalPlan>>;

/// Returns transformer action that need to be applied
pub type StageTransformer = Arc<dyn Fn(&LogicalPlan) -> Option<TransformerAction>>;

/// The Action that a transformer should take on the plan.
pub struct TransformerAction {
    /// list of plans that need to be applied to parent plans, in the order of parent to child.
    /// i.e. if this returns `[Projection, Aggregate]`, then the parent plan should be transformed to
    /// ```
    /// Original Parent Plan:
    ///     Projection:
    ///         Aggregate:
    ///             MergeScan: ...
    /// ```
    pub extra_parent_plans: Vec<LogicalPlan>,
    /// new child plan, if None, use the original plan.
    pub new_child_plan: Option<LogicalPlan>,
}

pub fn partial_commutative_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    Some(plan.clone())
}

#[cfg(test)]
mod test {
    use datafusion_expr::{LogicalPlanBuilder, Sort};

    use super::*;

    #[test]
    fn sort_on_empty_partition() {
        let plan = LogicalPlan::Sort(Sort {
            expr: vec![],
            input: Arc::new(LogicalPlanBuilder::empty(false).build().unwrap()),
            fetch: None,
        });
        assert!(matches!(
            Categorizer::check_plan(&plan, Some(vec![])),
            Commutativity::Commutative
        ));
    }
}
