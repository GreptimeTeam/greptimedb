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

use std::sync::Arc;

use datafusion_expr::{AggregateFunction, Expr, LogicalPlan, UserDefinedLogicalNode};

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
    pub fn check_plan(plan: &LogicalPlan) -> Commutativity {
        match plan {
            LogicalPlan::Projection(_) => Commutativity::Commutative,
            LogicalPlan::Filter(_) => Commutativity::Commutative,
            LogicalPlan::Window(_) => todo!(),
            LogicalPlan::Aggregate(_) => {
                // check all children exprs and uses the strictest level
                todo!()
            }
            LogicalPlan::Sort(_) => Commutativity::NonCommutative,
            LogicalPlan::Join(_) => Commutativity::NonCommutative,
            LogicalPlan::CrossJoin(_) => Commutativity::NonCommutative,
            LogicalPlan::Repartition(_) => todo!("unsupported? or non-commutative"),
            LogicalPlan::Union(_) => Commutativity::Unimplemented,
            LogicalPlan::TableScan(_) => Commutativity::NonCommutative,
            LogicalPlan::EmptyRelation(_) => Commutativity::NonCommutative,
            LogicalPlan::Subquery(_) => todo!(),
            LogicalPlan::SubqueryAlias(_) => todo!(),
            LogicalPlan::Limit(_) => Commutativity::PartialCommutative,
            LogicalPlan::Extension(extension) => {
                Self::check_extension_plan(extension.node.as_ref() as _)
            }
            LogicalPlan::Distinct(_) => Commutativity::Commutative,
            LogicalPlan::Unnest(_) => Commutativity::Commutative,
            LogicalPlan::Statement(_) => Commutativity::Unsupported,
            LogicalPlan::CreateExternalTable(_) => Commutativity::Unsupported,
            LogicalPlan::CreateMemoryTable(_) => Commutativity::Unsupported,
            LogicalPlan::CreateView(_) => Commutativity::Unsupported,
            LogicalPlan::CreateCatalogSchema(_) => Commutativity::Unsupported,
            LogicalPlan::CreateCatalog(_) => Commutativity::Unsupported,
            LogicalPlan::DropTable(_) => Commutativity::Unsupported,
            LogicalPlan::DropView(_) => Commutativity::Unsupported,
            LogicalPlan::Values(_) => Commutativity::Unsupported,
            LogicalPlan::Explain(_) => Commutativity::Unsupported,
            LogicalPlan::Analyze(_) => Commutativity::Unsupported,
            LogicalPlan::Prepare(_) => Commutativity::Unsupported,
            LogicalPlan::DescribeTable(_) => Commutativity::Unsupported,
            LogicalPlan::Dml(_) => Commutativity::Unsupported,
        }
    }

    pub fn check_extension_plan(plan: &dyn UserDefinedLogicalNode) -> Commutativity {
        todo!("enumerate all the extension plans here")
    }

    pub fn check_expr(expr: &Expr) -> Commutativity {
        match expr {
            Expr::Alias(_, _) => Commutativity::Commutative,
            Expr::Column(_) => Commutativity::Commutative,
            Expr::ScalarVariable(_, _) => Commutativity::Commutative,
            Expr::Literal(_) => Commutativity::Commutative,
            Expr::BinaryExpr(_) => todo!(),
            Expr::Like(_) => Commutativity::Commutative,
            Expr::ILike(_) => Commutativity::Commutative,
            Expr::SimilarTo(_) => Commutativity::Commutative,
            Expr::Not(_) => Commutativity::Commutative,
            Expr::IsNotNull(_) => Commutativity::Commutative,
            Expr::IsNull(_) => Commutativity::Commutative,
            Expr::IsTrue(_) => Commutativity::Commutative,
            Expr::IsFalse(_) => Commutativity::Commutative,
            Expr::IsUnknown(_) => Commutativity::Commutative,
            Expr::IsNotTrue(_) => Commutativity::Commutative,
            Expr::IsNotFalse(_) => Commutativity::Commutative,
            Expr::IsNotUnknown(_) => Commutativity::Commutative,
            Expr::Negative(_) => Commutativity::Commutative,
            Expr::GetIndexedField(_) => todo!(),
            Expr::Between(_) => Commutativity::Commutative,
            Expr::Case(_) => todo!(),
            Expr::Cast(_) => Commutativity::Commutative,
            Expr::TryCast(_) => Commutativity::Commutative,
            Expr::Sort(_) => todo!(),
            Expr::ScalarFunction { fun, args } => todo!(),
            Expr::ScalarUDF { fun, args } => todo!(),
            Expr::AggregateFunction(aggr_function) => {
                // Distinct requires the full set of data to be available
                if aggr_function.distinct {
                    return Commutativity::NonCommutative;
                }

                // todo: check parameter exprs
                match aggr_function.fun {
                    AggregateFunction::Count => todo!(),
                    AggregateFunction::Sum => todo!(),
                    AggregateFunction::Min => todo!(),
                    AggregateFunction::Max => todo!(),
                    AggregateFunction::Avg => todo!(),
                    AggregateFunction::Median => todo!(),
                    AggregateFunction::ApproxDistinct => todo!(),
                    AggregateFunction::ArrayAgg => todo!(),
                    AggregateFunction::Variance => todo!(),
                    AggregateFunction::VariancePop => todo!(),
                    AggregateFunction::Stddev => todo!(),
                    AggregateFunction::StddevPop => todo!(),
                    AggregateFunction::Covariance => todo!(),
                    AggregateFunction::CovariancePop => todo!(),
                    AggregateFunction::Correlation => todo!(),
                    AggregateFunction::ApproxPercentileCont => todo!(),
                    AggregateFunction::ApproxPercentileContWithWeight => todo!(),
                    AggregateFunction::ApproxMedian => todo!(),
                    AggregateFunction::Grouping => todo!(),
                }
            }
            Expr::WindowFunction(_) => todo!(),
            Expr::AggregateUDF { fun, args, filter } => todo!(),
            Expr::InList {
                expr,
                list,
                negated,
            } => todo!(),
            Expr::Exists { subquery, negated } => todo!(),
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => todo!(),
            Expr::ScalarSubquery(_) => todo!(),
            Expr::Wildcard => todo!(),
            Expr::QualifiedWildcard { qualifier } => todo!(),
            Expr::GroupingSet(_) => todo!(),
            Expr::Placeholder { id, data_type } => todo!(),
            Expr::OuterReferenceColumn(_, _) => todo!(),
        }
    }
}

pub type Transformer = Arc<dyn Fn(&LogicalPlan) -> Option<LogicalPlan>>;

fn partial_commutative_transformer(plan: &LogicalPlan) -> Option<LogicalPlan> {
    Some(plan.clone())
}

fn compile_test() {
    let mut transformers: Vec<Transformer> = vec![];
    transformers.push(Arc::new(partial_commutative_transformer));
}
