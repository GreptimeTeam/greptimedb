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

use datafusion_expr::{Expr, LogicalPlan, UserDefinedLogicalNode};
use promql::extension_plan::{
    EmptyMetric, InstantManipulate, RangeManipulate, SeriesDivide, SeriesNormalize,
};

use crate::dist_plan::merge_sort::{merge_sort_transformer, MergeSortLogicalPlan};
use crate::dist_plan::MergeScanLogicalPlan;

#[allow(dead_code)]
pub enum Commutativity {
    Commutative,
    PartialCommutative,
    ConditionalCommutative(Option<Transformer>),
    TransformedCommutative(Option<Transformer>),
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
                if !Self::check_partition(&aggr.group_expr, &partition_cols) {
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
            | Expr::InList(_) => Commutativity::Commutative,
            Expr::ScalarFunction(_udf) => Commutativity::Commutative,
            Expr::AggregateFunction(_udaf) => Commutativity::Commutative,

            Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotUnknown(_)
            | Expr::Case(_)
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
