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

//! Describes an aggregation function and it's input expression.

use datafusion_expr::function::AccumulatorArgs;
use datafusion_expr::AggregateUDF;
use datatypes::prelude::{ConcreteDataType, DataType};
use derive_where::derive_where;
pub(crate) use func::AggregateFunc;
use snafu::ResultExt;
pub use udaf::{OrderingReq, SortExpr};

use crate::error::DatafusionSnafu;
use crate::expr::relation::accum_v2::{AccumulatorV2, DfAccumulatorAdapter};
use crate::expr::{ScalarExpr, TypedExpr};
use crate::repr::RelationDesc;
use crate::Error;

mod accum;
mod accum_v2;
mod func;
mod udaf;

/// Describes an aggregation expression.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct AggregateExpr {
    /// Names the aggregation function.
    pub func: AggregateFunc,
    /// An expression which extracts from each row the input to `func`.
    /// TODO(discord9): currently unused in render phase(because AccumulablePlan remember each Aggr Expr's input/output column),
    /// so it only used in generate KeyValPlan from AggregateExpr
    pub expr: ScalarExpr,
    /// Should the aggregation be applied only to distinct results in each group.
    pub distinct: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd)]
#[derive_where(Ord)]
pub struct AggregateExprV2 {
    /// skipping `Ord` impl for func for convenience
    #[derive_where(skip)]
    pub func: AggregateUDF,
    /// should only be a simple column ref list
    pub args: Vec<TypedExpr>,
    /// Output / return type of this aggregate
    pub return_type: ConcreteDataType,
    pub name: String,
    /// The schema of the input relation to this aggregate
    pub schema: RelationDesc,
    // i.e. FIRST_VALUE(a ORDER BY b)
    pub ordering_req: OrderingReq,
    pub ignore_nulls: bool,
    pub is_distinct: bool,
    pub is_reversed: bool,
    /// The types of the arguments to this aggregate
    pub input_types: Vec<ConcreteDataType>,
    pub is_nullable: bool,
}

impl AggregateExprV2 {}

impl AggregateExprV2 {
    pub fn create_accumulator(&self) -> Result<Box<dyn AccumulatorV2>, Error> {
        let data_type = self.return_type.as_arrow_type();
        let schema = self.schema.to_df_schema()?;
        let ordering_req = self.ordering_req.to_lex_ordering(&schema)?;
        let exprs = self
            .args
            .iter()
            .map(|e| e.expr.as_physical_expr(&schema))
            .collect::<Result<Vec<_>, _>>()?;
        let accum_args = AccumulatorArgs {
            return_type: &data_type,
            schema: schema.as_arrow(),
            ignore_nulls: self.ignore_nulls,
            ordering_req: &ordering_req,
            is_reversed: self.is_reversed,
            name: &self.name,
            is_distinct: self.is_distinct,
            exprs: &exprs,
        };
        let acc = self.func.accumulator(accum_args).context(DatafusionSnafu {
            context: "Fail to build accumulator",
        })?;
        let acc = DfAccumulatorAdapter::new_unchecked(acc);
        Ok(Box::new(acc))
    }
}
