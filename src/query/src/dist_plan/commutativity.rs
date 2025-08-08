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

use common_function::aggrs::aggr_wrapper::{aggr_state_func_name, StateMergeHelper};
use common_function::function_registry::FUNCTION_REGISTRY;
use common_telemetry::debug;
use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

use crate::dist_plan::analyzer::AliasMapping;
use crate::dist_plan::merge_sort::{merge_sort_transformer, MergeSortLogicalPlan};
use crate::dist_plan::MergeScanLogicalPlan;

pub struct StepTransformAction {
    extra_parent_plans: Vec<LogicalPlan>,
    new_child_plan: Option<LogicalPlan>,
}

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
) -> datafusion_common::Result<StepTransformAction> {
    let LogicalPlan::Aggregate(input_aggr) = aggr_plan else {
        return Err(datafusion_common::DataFusionError::Plan(
            "step_aggr_to_upper_aggr only accepts Aggregate plan".to_string(),
        ));
    };
    if !is_all_aggr_exprs_steppable(&input_aggr.aggr_expr) {
        return Err(datafusion_common::DataFusionError::NotImplemented(format!(
            "Some aggregate expressions are not steppable in [{}]",
            input_aggr
                .aggr_expr
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }

    let step_aggr_plan = StateMergeHelper::split_aggr_node(input_aggr.clone())?;

    // TODO(discord9): remove duplication
    let ret = StepTransformAction {
        extra_parent_plans: vec![step_aggr_plan.upper_merge.clone()],
        new_child_plan: Some(step_aggr_plan.lower_state.clone()),
    };
    Ok(ret)
}

/// Check if the given aggregate expression is steppable.
/// As in if it can be split into multiple steps:
/// i.e. on datanode first call `state(input)` then
/// on frontend call `calc(merge(state))` to get the final result.
pub fn is_all_aggr_exprs_steppable(aggr_exprs: &[Expr]) -> bool {
    aggr_exprs.iter().all(|expr| {
        if let Some(aggr_func) = get_aggr_func(expr) {
            if aggr_func.params.distinct {
                // Distinct aggregate functions are not steppable(yet).
                return false;
            }

            // whether the corresponding state function exists in the registry
            FUNCTION_REGISTRY.is_aggr_func_exist(&aggr_state_func_name(aggr_func.func.name()))
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
    pub fn check_plan(plan: &LogicalPlan, partition_cols: Option<AliasMapping>) -> Commutativity {
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
                            ret.ok().map(|s| TransformerAction {
                                extra_parent_plans: s.extra_parent_plans,
                                new_child_plan: s.new_child_plan,
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
                // all group by expressions are partition columns can push down, unless
                // another push down(including `Limit` or `Sort`) is already in progress(which will then prvent next cond commutative node from being push down).
                // TODO(discord9): This is a temporary solution(that works), a better description of
                // commutativity is needed under this situation.
                Commutativity::ConditionalCommutative(None)
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
        partition_cols: &AliasMapping,
    ) -> Commutativity {
        match plan.name() {
            name if name == SeriesDivide::name() => {
                let series_divide = plan.as_any().downcast_ref::<SeriesDivide>().unwrap();
                let tags = series_divide.tags().iter().collect::<HashSet<_>>();

                for all_alias in partition_cols.values() {
                    let all_alias = all_alias.iter().map(|c| &c.name).collect::<HashSet<_>>();
                    if tags.intersection(&all_alias).count() == 0 {
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
        #[allow(deprecated)]
        match expr {
            Expr::Column(_)
            | Expr::ScalarVariable(_, _)
            | Expr::Literal(_, _)
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
    fn check_partition(exprs: &[Expr], partition_cols: &AliasMapping) -> bool {
        let mut ref_cols = HashSet::new();
        for expr in exprs {
            expr.add_column_refs(&mut ref_cols);
        }
        let ref_cols = ref_cols
            .into_iter()
            .map(|c| c.name.clone())
            .collect::<HashSet<_>>();
        for all_alias in partition_cols.values() {
            let all_alias = all_alias
                .iter()
                .map(|c| c.name.clone())
                .collect::<HashSet<_>>();
            // check if ref columns intersect with all alias of partition columns
            // is empty, if it's empty, not all partition columns show up in `exprs`
            if ref_cols.intersection(&all_alias).count() == 0 {
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
    /// ```ignore
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
            Categorizer::check_plan(&plan, Some(Default::default())),
            Commutativity::Commutative
        ));
    }
}
