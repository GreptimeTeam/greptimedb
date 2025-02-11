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

use std::fmt::Debug;

use common_query::prelude::Signature;
use datatypes::prelude::ConcreteDataType;

use crate::expr::relation::accum_v2::AccumulatorV2;
use crate::expr::ScalarExpr;
use crate::repr::RelationDesc;
use crate::Result;

/// User-defined aggregate function (UDAF) implementation.
/// All built-in UDAFs for flow is also impl by this trait.
pub trait AggrUDFImpl: Debug + Send + Sync {
    fn as_any(&self) -> &dyn std::any::Any;

    fn name(&self) -> &str;

    fn signature(&self) -> &Signature;
    /// What ConcreteDataType will be returned by this function, given the types of the arguments
    ///
    /// Keep the return_type's error type the same as `Function`'s return_type
    fn return_type(&self, arg_type: &[ConcreteDataType]) -> Result<ConcreteDataType>;

    fn accumulator(&self, acc_args: AccumulatorArgs<'_>) -> Result<Box<dyn AccumulatorV2>>;
}

/// contains information about how an aggregate function was called,
/// including the types of its arguments and any optional ordering expressions.
///
/// Should be created from AggregateExpr
pub struct AccumulatorArgs<'a> {
    pub return_type: &'a ConcreteDataType,
    pub schema: &'a RelationDesc,
    pub ignore_nulls: bool,
    /// The expressions in the `ORDER BY` clause passed to this aggregator.
    ///
    /// SQL allows the user to specify the ordering of arguments to the
    /// aggregate using an `ORDER BY`. For example:
    ///
    /// ```sql
    /// SELECT FIRST_VALUE(column1 ORDER BY column2) FROM t;
    /// ```
    ///
    /// If no `ORDER BY` is specified, `ordering_req` will be empty.
    pub ordering_req: &'a OrderingReq,
    pub is_reversed: bool,
    pub name: &'a str,
    pub is_distinct: bool,
    pub exprs: &'a [ScalarExpr],
}

#[derive(Debug, Clone)]
pub struct OrderingReq {
    pub exprs: Vec<SortExpr>,
}

#[derive(Debug, Clone)]
pub struct SortExpr {
    /// expression representing the column to sort
    pub expr: ScalarExpr,
    /// Whether to sort in descending order
    pub descending: bool,
    /// Whether to sort nulls first
    pub nulls_first: bool,
}
