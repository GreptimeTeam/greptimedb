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

use datafusion::functions_aggregate::sum::Sum;
use datafusion_expr::aggregate_function::AggregateFunction as BuiltInAggregateFunction;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionDefinition};
use datafusion_expr::utils::exprlist_to_columns;
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, UserDefinedLogicalNode};
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

use crate::dist_plan::merge_sort::{merge_sort_transformer, MergeSortLogicalPlan};
use crate::dist_plan::MergeScanLogicalPlan;

#[allow(dead_code)]
pub enum Commutativity<T> {
    Commutative,
    PartialCommutative,
    ConditionalCommutative(Option<Transformer<T>>),
    TransformedCommutative(Option<Transformer<T>>),
    NonCommutative,
    Unimplemented,
    /// For unrelated plans like DDL
    Unsupported,
}

impl<T> Commutativity<T> {
    /// Check if self is stricter than `lhs`
    fn is_stricter_than(&self, lhs: &Self) -> bool {
        match (lhs, self) {
            (Commutativity::Commutative, Commutativity::Commutative) => false,
            (Commutativity::Commutative, _) => true,

            (
                Commutativity::PartialCommutative,
                Commutativity::Commutative | Commutativity::PartialCommutative,
            ) => false,
            (Commutativity::PartialCommutative, _) => true,

            (
                Commutativity::ConditionalCommutative(_),
                Commutativity::Commutative
                | Commutativity::PartialCommutative
                | Commutativity::ConditionalCommutative(_),
            ) => false,
            (Commutativity::ConditionalCommutative(_), _) => true,

            (
                Commutativity::TransformedCommutative(_),
                Commutativity::Commutative
                | Commutativity::PartialCommutative
                | Commutativity::ConditionalCommutative(_)
                | Commutativity::TransformedCommutative(_),
            ) => false,
            (Commutativity::TransformedCommutative(_), _) => true,

            (
                Commutativity::NonCommutative
                | Commutativity::Unimplemented
                | Commutativity::Unsupported,
                _,
            ) => false,
        }
    }

    /// Return a bare commutative level without any transformer
    fn bare_level<To>(&self) -> Commutativity<To> {
        match self {
            Commutativity::Commutative => Commutativity::Commutative,
            Commutativity::PartialCommutative => Commutativity::PartialCommutative,
            Commutativity::ConditionalCommutative(_) => Commutativity::ConditionalCommutative(None),
            Commutativity::TransformedCommutative(_) => Commutativity::TransformedCommutative(None),
            Commutativity::NonCommutative => Commutativity::NonCommutative,
            Commutativity::Unimplemented => Commutativity::Unimplemented,
            Commutativity::Unsupported => Commutativity::Unsupported,
        }
    }
}

impl<T> std::fmt::Debug for Commutativity<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Commutativity::Commutative => write!(f, "Commutative"),
            Commutativity::PartialCommutative => write!(f, "PartialCommutative"),
            Commutativity::ConditionalCommutative(_) => write!(f, "ConditionalCommutative"),
            Commutativity::TransformedCommutative(_) => write!(f, "TransformedCommutative"),
            Commutativity::NonCommutative => write!(f, "NonCommutative"),
            Commutativity::Unimplemented => write!(f, "Unimplemented"),
            Commutativity::Unsupported => write!(f, "Unsupported"),
        }
    }
}

pub struct Categorizer {}

impl Categorizer {
    pub fn check_plan(
        plan: &LogicalPlan,
        partition_cols: Option<Vec<String>>,
    ) -> Commutativity<LogicalPlan> {
        let partition_cols = partition_cols.unwrap_or_default();

        match plan {
            LogicalPlan::Projection(proj) => {
                for expr in &proj.expr {
                    let commutativity = Self::check_expr(expr);
                    if !matches!(commutativity, Commutativity::Commutative) {
                        return commutativity.bare_level();
                    }
                }
                Commutativity::Commutative
            }
            // TODO(ruihang): Change this to Commutative once Like is supported in substrait
            LogicalPlan::Filter(filter) => Self::check_expr(&filter.predicate).bare_level(),
            LogicalPlan::Window(_) => Commutativity::Unimplemented,
            LogicalPlan::Aggregate(aggr) => {
                // fast path: if the group_expr is a subset of partition_cols
                if Self::check_partition(&aggr.group_expr, &partition_cols) {
                    return Commutativity::Commutative;
                }

                common_telemetry::info!("[DEBUG] aggregate plan expr: {:?}", aggr.aggr_expr);

                // get all commutativity levels of aggregate exprs and find the strictest one
                let aggr_expr_comm = aggr
                    .aggr_expr
                    .iter()
                    .map(Self::check_expr)
                    .collect::<Vec<_>>();
                let mut strictest = Commutativity::Commutative;
                for comm in &aggr_expr_comm {
                    if comm.is_stricter_than(&strictest) {
                        strictest = comm.bare_level();
                    }
                }

                common_telemetry::info!("[DEBUG] aggr_expr_comm: {:?}", aggr_expr_comm);
                common_telemetry::info!("[DEBUG] strictest: {:?}", strictest);

                // fast path: if any expr is commutative or non-commutative
                if matches!(
                    strictest,
                    Commutativity::Commutative
                        | Commutativity::NonCommutative
                        | Commutativity::Unimplemented
                        | Commutativity::Unsupported
                ) {
                    return strictest.bare_level();
                }

                common_telemetry::info!("[DEBUG] continue for strictest",);

                // collect expr transformers
                let mut expr_transformer = Vec::with_capacity(aggr.aggr_expr.len());
                for expr_comm in aggr_expr_comm {
                    match expr_comm {
                        Commutativity::Commutative => expr_transformer.push(None),
                        Commutativity::ConditionalCommutative(transformer) => {
                            expr_transformer.push(transformer.clone());
                        }
                        Commutativity::PartialCommutative => expr_transformer
                            .push(Some(Arc::new(expr_partial_commutative_transformer))),
                        _ => expr_transformer.push(None),
                    }
                }

                // build plan transformer
                let transformer = Arc::new(move |plan: &LogicalPlan| {
                    if let LogicalPlan::Aggregate(aggr) = plan {
                        let mut new_plan = aggr.clone();

                        // transform aggr exprs
                        for (expr, transformer) in
                            new_plan.aggr_expr.iter_mut().zip(&expr_transformer)
                        {
                            if let Some(transformer) = transformer {
                                let new_expr = transformer(expr)?;
                                *expr = new_expr;
                            }
                        }

                        // transform group exprs
                        for expr in new_plan.group_expr.iter_mut() {
                            // if let Some(transformer) = transformer {
                            //     let new_expr = transformer(expr)?;
                            //     *expr = new_expr;
                            // }
                            let expr_name = expr.name_for_alias().expect("not a sort expr");
                            *expr = Expr::Column(expr_name.into());
                        }

                        common_telemetry::info!(
                            "[DEBUG] new plan aggr expr: {:?}, group expr: {:?}",
                            new_plan.aggr_expr,
                            new_plan.group_expr
                        );
                        Some(LogicalPlan::Aggregate(new_plan))
                    } else {
                        None
                    }
                });

                common_telemetry::info!("[DEBUG] done TransformedCommutative for aggr plan ");

                Commutativity::TransformedCommutative(Some(transformer))
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
            LogicalPlan::CrossJoin(_) => Commutativity::NonCommutative,
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
                } else if limit.skip == 0 && limit.fetch.is_some() {
                    Commutativity::PartialCommutative
                } else {
                    Commutativity::Unimplemented
                }
            }
            LogicalPlan::Extension(extension) => {
                Self::check_extension_plan(extension.node.as_ref() as _)
            }
            LogicalPlan::Distinct(_) => Commutativity::Unimplemented,
            LogicalPlan::Unnest(_) => Commutativity::Commutative,
            LogicalPlan::Statement(_) => Commutativity::Unsupported,
            LogicalPlan::Values(_) => Commutativity::Unsupported,
            LogicalPlan::Explain(_) => Commutativity::Unsupported,
            LogicalPlan::Analyze(_) => Commutativity::Unsupported,
            LogicalPlan::Prepare(_) => Commutativity::Unsupported,
            LogicalPlan::DescribeTable(_) => Commutativity::Unsupported,
            LogicalPlan::Dml(_) => Commutativity::Unsupported,
            LogicalPlan::Ddl(_) => Commutativity::Unsupported,
            LogicalPlan::Copy(_) => Commutativity::Unsupported,
            LogicalPlan::RecursiveQuery(_) => Commutativity::Unsupported,
        }
    }

    pub fn check_extension_plan(plan: &dyn UserDefinedLogicalNode) -> Commutativity<LogicalPlan> {
        match plan.name() {
            name if name == EmptyMetric::name()
                || name == InstantManipulate::name()
                || name == SeriesNormalize::name()
                || name == RangeManipulate::name()
                || name == SeriesDivide::name()
                || name == MergeScanLogicalPlan::name()
                || name == MergeSortLogicalPlan::name() =>
            {
                Commutativity::Unimplemented
            }
            _ => Commutativity::Unsupported,
        }
    }

    pub fn check_expr(expr: &Expr) -> Commutativity<Expr> {
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
            | Expr::Sort(_)
            | Expr::Exists(_)
            | Expr::ScalarFunction(_) => Commutativity::Commutative,

            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::WindowFunction(_)
            | Expr::InList(_)
            | Expr::InSubquery(_)
            | Expr::ScalarSubquery(_)
            | Expr::Wildcard { .. } => Commutativity::Unimplemented,

            Expr::AggregateFunction(aggr_fn) => Self::check_aggregate_fn(aggr_fn),

            Expr::Alias(_)
            | Expr::Unnest(_)
            | Expr::GroupingSet(_)
            | Expr::Placeholder(_)
            | Expr::OuterReferenceColumn(_, _) => Commutativity::Unimplemented,
        }
    }

    fn check_aggregate_fn(aggr_fn: &AggregateFunction) -> Commutativity<Expr> {
        common_telemetry::info!("[DEBUG] checking aggr_fn: {:?}", aggr_fn);
        match &aggr_fn.func_def {
            AggregateFunctionDefinition::BuiltIn(func_def) => match func_def {
                BuiltInAggregateFunction::Max | BuiltInAggregateFunction::Min => {
                    // Commutativity::PartialCommutative
                    common_telemetry::info!("[DEBUG] checking min/max: {:?}", aggr_fn);
                    let mut new_fn = aggr_fn.clone();
                    let col_name = Expr::AggregateFunction(aggr_fn.clone())
                        .name_for_alias()
                        .expect("not a sort expr");
                    let alias = col_name.clone();
                    new_fn.args = vec![Expr::Column(col_name.into())];

                    // new_fn.func_def =
                    //     AggregateFunctionDefinition::BuiltIn(BuiltInAggregateFunction::Sum);
                    Commutativity::ConditionalCommutative(Some(Arc::new(move |_| {
                        common_telemetry::info!("[DEBUG] transforming min/max fn: {:?}", new_fn);
                        Some(Expr::AggregateFunction(new_fn.clone()).alias(alias.clone()))
                    })))
                }
                BuiltInAggregateFunction::Count => {
                    common_telemetry::info!("[DEBUG] checking count_fn: {:?}", aggr_fn);
                    let col_name = Expr::AggregateFunction(aggr_fn.clone())
                        .name_for_alias()
                        .expect("not a sort expr");
                    let sum_udf = Arc::new(AggregateUDF::new_from_impl(Sum::new()));
                    let alias = col_name.clone();
                    // let sum_func = Arc::new(AggregateFunction::new_udf(
                    //     sum_udf,
                    //     vec![Expr::Column(col_name.into())],
                    //     false,
                    //     None,
                    //     None,
                    //     None,
                    // ));
                    let mut sum_expr = aggr_fn.clone();
                    sum_expr.func_def = AggregateFunctionDefinition::UDF(sum_udf);
                    sum_expr.args = vec![Expr::Column(col_name.into())];
                    // let mut sum_fn = aggr_fn.clone();
                    // sum_fn.func_def =
                    //     AggregateFunctionDefinition::BuiltIn(BuiltInAggregateFunction::Sum);
                    Commutativity::ConditionalCommutative(Some(Arc::new(move |_| {
                        common_telemetry::info!("[DEBUG] transforming sum_fn: {:?}", sum_expr);
                        Some(Expr::AggregateFunction(sum_expr.clone()).alias(alias.clone()))
                    })))
                }
                _ => Commutativity::Unimplemented,
            },
            AggregateFunctionDefinition::UDF(_) => Commutativity::Unimplemented,
        }
    }

    /// Return true if the given expr and partition cols satisfied the rule.
    /// In this case the plan can be treated as fully commutative.
    fn check_partition(exprs: &[Expr], partition_cols: &[String]) -> bool {
        let mut ref_cols = HashSet::new();
        if exprlist_to_columns(exprs, &mut ref_cols).is_err() {
            return false;
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

pub type Transformer<T> = Arc<dyn for<'a> Fn(&'a T) -> Option<T>>;

pub fn partial_commutative_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    Some(plan.clone())
}

pub fn expr_partial_commutative_transformer(expr: &Expr) -> Option<Expr> {
    Some(expr.clone())
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
