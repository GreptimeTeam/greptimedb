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

//! Scalar expressions.

use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{make_array, ArrayData, ArrayRef};
use common_error::ext::BoxedError;
use datatypes::prelude::{ConcreteDataType, DataType};
use datatypes::value::Value;
use datatypes::vectors::{BooleanVector, Helper, VectorRef};
use hydroflow::lattices::cc_traits::Iter;
use itertools::Itertools;
use snafu::{ensure, OptionExt, ResultExt};

use crate::error::{
    DatafusionSnafu, Error, InvalidQuerySnafu, UnexpectedSnafu, UnsupportedTemporalFilterSnafu,
};
use crate::expr::error::{
    ArrowSnafu, DataTypeSnafu, EvalError, InvalidArgumentSnafu, OptimizeSnafu, TypeMismatchSnafu,
};
use crate::expr::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use crate::expr::{Batch, DfScalarFunction};
use crate::repr::ColumnType;
/// A scalar expression with a known type.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Hash)]
pub struct TypedExpr {
    /// The expression.
    pub expr: ScalarExpr,
    /// The type of the expression.
    pub typ: ColumnType,
}

impl TypedExpr {
    pub fn new(expr: ScalarExpr, typ: ColumnType) -> Self {
        Self { expr, typ }
    }
}

/// A scalar expression, which can be evaluated to a value.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// Extra type info to know original type even when it is null
    Literal(Value, ConcreteDataType),
    /// A call to an unmaterializable function.
    ///
    /// These functions cannot be evaluated by `ScalarExpr::eval`. They must
    /// be transformed away by a higher layer.
    CallUnmaterializable(UnmaterializableFunc),
    CallUnary {
        func: UnaryFunc,
        expr: Box<ScalarExpr>,
    },
    CallBinary {
        func: BinaryFunc,
        expr1: Box<ScalarExpr>,
        expr2: Box<ScalarExpr>,
    },
    CallVariadic {
        func: VariadicFunc,
        exprs: Vec<ScalarExpr>,
    },
    CallDf {
        /// invariant: the input args set inside this [`DfScalarFunction`] is
        /// always col(0) to col(n-1) where n is the length of `expr`
        df_scalar_fn: DfScalarFunction,
        exprs: Vec<ScalarExpr>,
    },
    /// Conditionally evaluated expressions.
    ///
    /// It is important that `then` and `els` only be evaluated if
    /// `cond` is true or not, respectively. This is the only way
    /// users can guard execution (other logical operator do not
    /// short-circuit) and we need to preserve that.
    If {
        cond: Box<ScalarExpr>,
        then: Box<ScalarExpr>,
        els: Box<ScalarExpr>,
    },
}

impl ScalarExpr {
    pub fn with_type(self, typ: ColumnType) -> TypedExpr {
        TypedExpr::new(self, typ)
    }

    /// try to determine the type of the expression
    pub fn typ(&self, context: &[ColumnType]) -> Result<ColumnType, Error> {
        match self {
            ScalarExpr::Column(i) => context.get(*i).cloned().ok_or_else(|| {
                UnexpectedSnafu {
                    reason: format!("column index {} out of range of len={}", i, context.len()),
                }
                .build()
            }),
            ScalarExpr::Literal(_, typ) => Ok(ColumnType::new_nullable(typ.clone())),
            ScalarExpr::CallUnmaterializable(func) => {
                Ok(ColumnType::new_nullable(func.signature().output))
            }
            ScalarExpr::CallUnary { func, .. } => {
                Ok(ColumnType::new_nullable(func.signature().output))
            }
            ScalarExpr::CallBinary { func, .. } => {
                Ok(ColumnType::new_nullable(func.signature().output))
            }
            ScalarExpr::CallVariadic { func, .. } => {
                Ok(ColumnType::new_nullable(func.signature().output))
            }
            ScalarExpr::If { then, .. } => then.typ(context),
            ScalarExpr::CallDf { df_scalar_fn, .. } => {
                let arrow_typ = df_scalar_fn
                    .fn_impl
                    // TODO(discord9): get scheme from args instead?
                    .data_type(df_scalar_fn.df_schema.as_arrow())
                    .context({
                        DatafusionSnafu {
                            context: "Failed to get data type from datafusion scalar function",
                        }
                    })?;
                let typ = ConcreteDataType::try_from(&arrow_typ)
                    .map_err(BoxedError::new)
                    .context(crate::error::ExternalSnafu)?;
                Ok(ColumnType::new_nullable(typ))
            }
        }
    }
}

impl ScalarExpr {
    pub fn cast(self, typ: ConcreteDataType) -> Self {
        ScalarExpr::CallUnary {
            func: UnaryFunc::Cast(typ),
            expr: Box::new(self),
        }
    }

    /// apply optimization to the expression, like flatten variadic function
    pub fn optimize(&mut self) {
        self.flatten_varidic_fn();
    }

    /// Because Substrait's `And`/`Or` function is binary, but FlowPlan's
    /// `And`/`Or` function is variadic, we need to flatten the `And` function if multiple `And`/`Or` functions are nested.
    fn flatten_varidic_fn(&mut self) {
        if let ScalarExpr::CallVariadic { func, exprs } = self {
            let mut new_exprs = vec![];
            for expr in std::mem::take(exprs) {
                if let ScalarExpr::CallVariadic {
                    func: inner_func,
                    exprs: mut inner_exprs,
                } = expr
                {
                    if *func == inner_func {
                        for inner_expr in inner_exprs.iter_mut() {
                            inner_expr.flatten_varidic_fn();
                        }
                        new_exprs.extend(inner_exprs);
                    }
                } else {
                    new_exprs.push(expr);
                }
            }
            *exprs = new_exprs;
        }
    }
}

impl ScalarExpr {
    /// Call a unary function on this expression.
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    /// Call a binary function on this expression and another.
    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn eval_batch(&self, batch: &Batch) -> Result<VectorRef, EvalError> {
        match self {
            ScalarExpr::Column(i) => Ok(batch.batch()[*i].clone()),
            ScalarExpr::Literal(val, dt) => Ok(Helper::try_from_scalar_value(
                val.try_to_scalar_value(dt).context(DataTypeSnafu {
                    msg: "Failed to convert literal to scalar value",
                })?,
                batch.row_count(),
            )
            .context(DataTypeSnafu {
                msg: "Failed to convert scalar value to vector ref when parsing literal",
            })?),
            ScalarExpr::CallUnmaterializable(_) => OptimizeSnafu {
                reason: "Can't eval unmaterializable function",
            }
            .fail()?,
            ScalarExpr::CallUnary { func, expr } => func.eval_batch(batch, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => func.eval_batch(batch, expr1, expr2),
            ScalarExpr::CallVariadic { func, exprs } => func.eval_batch(batch, exprs),
            ScalarExpr::CallDf {
                df_scalar_fn,
                exprs,
            } => df_scalar_fn.eval_batch(batch, exprs),
            ScalarExpr::If { cond, then, els } => Self::eval_if_then(batch, cond, then, els),
        }
    }

    /// NOTE: this if then eval impl assume all given expr are pure, and will not change the state of the world
    /// since it will evaluate both then and else branch and filter the result
    fn eval_if_then(
        batch: &Batch,
        cond: &ScalarExpr,
        then: &ScalarExpr,
        els: &ScalarExpr,
    ) -> Result<VectorRef, EvalError> {
        let conds = cond.eval_batch(batch)?;
        let bool_conds = conds
            .as_any()
            .downcast_ref::<BooleanVector>()
            .context({
                TypeMismatchSnafu {
                    expected: ConcreteDataType::boolean_datatype(),
                    actual: conds.data_type(),
                }
            })?
            .as_boolean_array();

        let indices = bool_conds
            .into_iter()
            .enumerate()
            .map(|(idx, b)| {
                (
                    match b {
                        Some(true) => 0,  // then branch vector
                        Some(false) => 1, // else branch vector
                        None => 2,        // null vector
                    },
                    idx,
                )
            })
            .collect_vec();

        let then_input_vec = then.eval_batch(batch)?;
        let else_input_vec = els.eval_batch(batch)?;

        ensure!(
            then_input_vec.data_type() == else_input_vec.data_type(),
            TypeMismatchSnafu {
                expected: then_input_vec.data_type(),
                actual: else_input_vec.data_type(),
            }
        );

        ensure!(
            then_input_vec.len() == else_input_vec.len() && then_input_vec.len() == batch.row_count(),
            InvalidArgumentSnafu {
                reason: format!(
                    "then and else branch must have the same length(found {} and {}) which equals input batch's row count(which is {})",
                    then_input_vec.len(),
                    else_input_vec.len(),
                    batch.row_count()
                )
            }
        );

        fn new_nulls(dt: &arrow_schema::DataType, len: usize) -> ArrayRef {
            let data = ArrayData::new_null(dt, len);
            make_array(data)
        }

        let null_input_vec = new_nulls(
            &then_input_vec.data_type().as_arrow_type(),
            batch.row_count(),
        );

        let interleave_values = vec![
            then_input_vec.to_arrow_array(),
            else_input_vec.to_arrow_array(),
            null_input_vec,
        ];
        let int_ref: Vec<_> = interleave_values.iter().map(|x| x.as_ref()).collect();

        let interleave_res_arr =
            arrow::compute::interleave(&int_ref, &indices).context(ArrowSnafu {
                context: "Failed to interleave output arrays",
            })?;
        let res_vec = Helper::try_into_vector(interleave_res_arr).context(DataTypeSnafu {
            msg: "Failed to convert arrow array to vector",
        })?;
        Ok(res_vec)
    }

    /// Eval this expression with the given values.
    pub fn eval(&self, values: &[Value]) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => Ok(values[*index].clone()),
            ScalarExpr::Literal(row_res, _ty) => Ok(row_res.clone()),
            ScalarExpr::CallUnmaterializable(_) => OptimizeSnafu {
                reason: "Can't eval unmaterializable function".to_string(),
            }
            .fail(),
            ScalarExpr::CallUnary { func, expr } => func.eval(values, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => func.eval(values, expr1, expr2),
            ScalarExpr::CallVariadic { func, exprs } => func.eval(values, exprs),
            ScalarExpr::If { cond, then, els } => match cond.eval(values) {
                Ok(Value::Boolean(true)) => then.eval(values),
                Ok(Value::Boolean(false)) => els.eval(values),
                _ => InvalidArgumentSnafu {
                    reason: "if condition must be boolean".to_string(),
                }
                .fail(),
            },
            ScalarExpr::CallDf {
                df_scalar_fn,
                exprs,
            } => df_scalar_fn.eval(values, exprs),
        }
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) -> Result<(), Error> {
        // check first so that we don't end up with a partially permuted expression
        ensure!(
            self.get_all_ref_columns()
                .into_iter()
                .all(|i| i < permutation.len()),
            InvalidQuerySnafu {
                reason: format!(
                    "permutation {:?} is not a valid permutation for expression {:?}",
                    permutation, self
                ),
            }
        );

        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
            Ok(())
        })?;
        Ok(())
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &BTreeMap<usize, usize>) -> Result<(), Error> {
        // check first so that we don't end up with a partially permuted expression
        ensure!(
            self.get_all_ref_columns()
                .is_subset(&permutation.keys().cloned().collect()),
            InvalidQuerySnafu {
                reason: format!(
                    "permutation {:?} is not a valid permutation for expression {:?}",
                    permutation, self
                ),
            }
        );

        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
            Ok(())
        })
    }

    /// Returns the set of columns that are referenced by `self`.
    pub fn get_all_ref_columns(&self) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        self.visit_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
            Ok(())
        })
        .unwrap();
        support
    }

    /// Return true if the expression is a column reference.
    pub fn is_column(&self) -> bool {
        matches!(self, ScalarExpr::Column(_))
    }

    /// Cast the expression to a column reference if it is one.
    pub fn as_column(&self) -> Option<usize> {
        if let ScalarExpr::Column(i) = self {
            Some(*i)
        } else {
            None
        }
    }

    /// Cast the expression to a literal if it is one.
    pub fn as_literal(&self) -> Option<Value> {
        if let ScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.clone())
        } else {
            None
        }
    }

    /// Return true if the expression is a literal.
    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(..))
    }

    /// Return true if the expression is a literal true.
    pub fn is_literal_true(&self) -> bool {
        Some(Value::Boolean(true)) == self.as_literal()
    }

    /// Return true if the expression is a literal false.
    pub fn is_literal_false(&self) -> bool {
        Some(Value::Boolean(false)) == self.as_literal()
    }

    /// Return true if the expression is a literal null.
    pub fn is_literal_null(&self) -> bool {
        Some(Value::Null) == self.as_literal()
    }

    /// Build a literal null
    pub fn literal_null() -> Self {
        ScalarExpr::Literal(Value::Null, ConcreteDataType::null_datatype())
    }

    /// Build a literal from value and type
    pub fn literal(res: Value, typ: ConcreteDataType) -> Self {
        ScalarExpr::Literal(res, typ)
    }

    /// Build a literal false
    pub fn literal_false() -> Self {
        ScalarExpr::Literal(Value::Boolean(false), ConcreteDataType::boolean_datatype())
    }

    /// Build a literal true
    pub fn literal_true() -> Self {
        ScalarExpr::Literal(Value::Boolean(true), ConcreteDataType::boolean_datatype())
    }
}

impl ScalarExpr {
    /// visit post-order without stack call limit, but may cause stack overflow
    fn visit_post_nolimit<F>(&self, f: &mut F) -> Result<(), EvalError>
    where
        F: FnMut(&Self) -> Result<(), EvalError>,
    {
        self.visit_children(|e| e.visit_post_nolimit(f))?;
        f(self)
    }

    fn visit_children<F>(&self, mut f: F) -> Result<(), EvalError>
    where
        F: FnMut(&Self) -> Result<(), EvalError>,
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => Ok(()),
            ScalarExpr::CallUnary { expr, .. } => f(expr),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
                Ok(())
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)
            }
            ScalarExpr::CallDf {
                df_scalar_fn: _,
                exprs,
            } => {
                for expr in exprs {
                    f(expr)?;
                }
                Ok(())
            }
        }
    }

    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F) -> Result<(), Error>
    where
        F: FnMut(&mut Self) -> Result<(), Error>,
    {
        self.visit_mut_children(|e: &mut Self| e.visit_mut_post_nolimit(f))?;
        f(self)
    }

    fn visit_mut_children<F>(&mut self, mut f: F) -> Result<(), Error>
    where
        F: FnMut(&mut Self) -> Result<(), Error>,
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => Ok(()),
            ScalarExpr::CallUnary { expr, .. } => f(expr),
            ScalarExpr::CallBinary { expr1, expr2, .. } => {
                f(expr1)?;
                f(expr2)
            }
            ScalarExpr::CallVariadic { exprs, .. } => {
                for expr in exprs {
                    f(expr)?;
                }
                Ok(())
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond)?;
                f(then)?;
                f(els)
            }
            ScalarExpr::CallDf {
                df_scalar_fn: _,
                exprs,
            } => {
                for expr in exprs {
                    f(expr)?;
                }
                Ok(())
            }
        }
    }
}

impl ScalarExpr {
    /// if expr contains function `Now`
    pub fn contains_temporal(&self) -> bool {
        let mut contains = false;
        self.visit_post_nolimit(&mut |e| {
            if let ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now) = e {
                contains = true;
            }
            Ok(())
        })
        .unwrap();
        contains
    }

    /// extract lower or upper bound of `Now` for expr, where `lower bound <= expr < upper bound`
    ///
    /// returned bool indicates whether the bound is upper bound:
    ///
    /// false for lower bound, true for upper bound
    /// TODO(discord9): allow simple transform like `now() + a < b` to `now() < b - a`
    pub fn extract_bound(&self) -> Result<(Option<Self>, Option<Self>), Error> {
        let unsupported_err = |msg: &str| {
            UnsupportedTemporalFilterSnafu {
                reason: msg.to_string(),
            }
            .fail()
        };

        let Self::CallBinary {
            mut func,
            mut expr1,
            mut expr2,
        } = self.clone()
        else {
            return unsupported_err("Not a binary expression");
        };

        // TODO(discord9): support simple transform like `now() + a < b` to `now() < b - a`

        let expr1_is_now = *expr1 == ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now);
        let expr2_is_now = *expr2 == ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now);

        if !(expr1_is_now ^ expr2_is_now) {
            return unsupported_err("None of the sides of the comparison is `now()`");
        }

        if expr2_is_now {
            std::mem::swap(&mut expr1, &mut expr2);
            func = BinaryFunc::reverse_compare(&func)?;
        }

        let step = |expr: ScalarExpr| expr.call_unary(UnaryFunc::StepTimestamp);
        match func {
            // now == expr2 -> now <= expr2 && now < expr2 + 1
            BinaryFunc::Eq => Ok((Some(*expr2.clone()), Some(step(*expr2)))),
            // now < expr2 -> now < expr2
            BinaryFunc::Lt => Ok((None, Some(*expr2))),
            // now <= expr2 -> now < expr2 + 1
            BinaryFunc::Lte => Ok((None, Some(step(*expr2)))),
            // now > expr2 -> now >= expr2 + 1
            BinaryFunc::Gt => Ok((Some(step(*expr2)), None)),
            // now >= expr2 -> now >= expr2
            BinaryFunc::Gte => Ok((Some(*expr2), None)),
            _ => unreachable!("Already checked"),
        }
    }
}

#[cfg(test)]
mod test {
    use datatypes::vectors::{Int32Vector, Vector};
    use pretty_assertions::assert_eq;

    use super::*;

    #[test]
    fn test_extract_bound() {
        let test_list: [(ScalarExpr, Result<_, EvalError>); 5] = [
            // col(0) == now
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Eq,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    Some(ScalarExpr::Column(0)),
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                )),
            ),
            // now < col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Lt,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((None, Some(ScalarExpr::Column(0)))),
            ),
            // now <= col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Lte,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    None,
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                )),
            ),
            // now > col(0) -> now >= col(0) + 1
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Gt,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((
                    Some(ScalarExpr::CallUnary {
                        func: UnaryFunc::StepTimestamp,
                        expr: Box::new(ScalarExpr::Column(0)),
                    }),
                    None,
                )),
            ),
            // now >= col(0)
            (
                ScalarExpr::CallBinary {
                    func: BinaryFunc::Gte,
                    expr1: Box::new(ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)),
                    expr2: Box::new(ScalarExpr::Column(0)),
                },
                Ok((Some(ScalarExpr::Column(0)), None)),
            ),
        ];
        for (expr, expected) in test_list.into_iter() {
            let actual = expr.extract_bound();
            // EvalError is not Eq, so we need to compare the error message
            match (actual, expected) {
                (Ok(l), Ok(r)) => assert_eq!(l, r),
                (l, r) => panic!("expected: {:?}, actual: {:?}", r, l),
            }
        }
    }

    #[test]
    fn test_bad_permute() {
        let mut expr = ScalarExpr::Column(4);
        let permutation = vec![1, 2, 3];
        let res = expr.permute(&permutation);
        assert!(matches!(res, Err(Error::InvalidQuery { .. })));

        let mut expr = ScalarExpr::Column(0);
        let permute_map = BTreeMap::from([(1, 2), (3, 4)]);
        let res = expr.permute_map(&permute_map);
        assert!(matches!(res, Err(Error::InvalidQuery { .. })));
    }

    #[test]
    fn test_eval_batch_if_then() {
        // TODO(discord9): add more tests
        {
            let expr = ScalarExpr::If {
                cond: Box::new(ScalarExpr::Column(0).call_binary(
                    ScalarExpr::literal(Value::from(0), ConcreteDataType::int32_datatype()),
                    BinaryFunc::Eq,
                )),
                then: Box::new(ScalarExpr::literal(
                    Value::from(42),
                    ConcreteDataType::int32_datatype(),
                )),
                els: Box::new(ScalarExpr::literal(
                    Value::from(37),
                    ConcreteDataType::int32_datatype(),
                )),
            };
            let raw = vec![
                None,
                Some(0),
                Some(1),
                None,
                None,
                Some(0),
                Some(0),
                Some(1),
                Some(1),
            ];
            let raw_len = raw.len();
            let vectors = vec![Int32Vector::from(raw).slice(0, raw_len)];

            let batch = Batch::try_new(vectors, raw_len).unwrap();
            let expected = Int32Vector::from(vec![
                None,
                Some(42),
                Some(37),
                None,
                None,
                Some(42),
                Some(42),
                Some(37),
                Some(37),
            ])
            .slice(0, raw_len);
            assert_eq!(expr.eval_batch(&batch).unwrap(), expected);

            let raw = vec![Some(0)];
            let raw_len = raw.len();
            let vectors = vec![Int32Vector::from(raw).slice(0, raw_len)];

            let batch = Batch::try_new(vectors, raw_len).unwrap();
            let expected = Int32Vector::from(vec![Some(42)]).slice(0, raw_len);
            assert_eq!(expr.eval_batch(&batch).unwrap(), expected);

            let raw: Vec<Option<i32>> = vec![];
            let raw_len = raw.len();
            let vectors = vec![Int32Vector::from(raw).slice(0, raw_len)];

            let batch = Batch::try_new(vectors, raw_len).unwrap();
            let expected = Int32Vector::from(vec![]).slice(0, raw_len);
            assert_eq!(expr.eval_batch(&batch).unwrap(), expected);
        }
    }
}
