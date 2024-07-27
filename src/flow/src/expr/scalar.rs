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
use std::sync::{Arc, Mutex};

use bytes::BytesMut;
use common_error::ext::BoxedError;
use common_recordbatch::DfRecordBatch;
use common_telemetry::debug;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::data_type::DataType;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use datatypes::{arrow_array, value};
use prost::Message;
use serde::{Deserialize, Serialize};
use snafu::{ensure, ResultExt};
use substrait::error::{DecodeRelSnafu, EncodeRelSnafu};
use substrait::substrait_proto_df::proto::expression::{RexType, ScalarFunction};
use substrait::substrait_proto_df::proto::Expression;

use crate::error::{
    DatafusionSnafu, Error, InvalidQuerySnafu, UnexpectedSnafu, UnsupportedTemporalFilterSnafu,
};
use crate::expr::error::{
    ArrowSnafu, DatafusionSnafu as EvalDatafusionSnafu, EvalError, ExternalSnafu,
    InvalidArgumentSnafu, OptimizeSnafu,
};
use crate::expr::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};
use crate::repr::{ColumnType, RelationDesc, RelationType};
use crate::transform::{from_scalar_fn_to_df_fn_impl, FunctionExtensions};
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

impl TypedExpr {
    /// expand multi-value expression to multiple expressions with new indices
    ///
    /// Currently it just mean expand `TumbleWindow` to `TumbleWindowFloor` and `TumbleWindowCeiling`
    ///
    /// TODO(discord9): test if nested reduce combine with df scalar function would cause problem
    pub fn expand_multi_value(
        input_typ: &RelationType,
        exprs: &[TypedExpr],
    ) -> Result<Vec<TypedExpr>, Error> {
        // old indices in mfp, expanded expr
        let mut ret = vec![];
        let input_arity = input_typ.column_types.len();
        for (old_idx, expr) in exprs.iter().enumerate() {
            if let ScalarExpr::CallUnmaterializable(UnmaterializableFunc::TumbleWindow {
                ts,
                window_size,
                start_time,
            }) = &expr.expr
            {
                let floor = UnaryFunc::TumbleWindowFloor {
                    window_size: *window_size,
                    start_time: *start_time,
                };
                let ceil = UnaryFunc::TumbleWindowCeiling {
                    window_size: *window_size,
                    start_time: *start_time,
                };
                let floor = ScalarExpr::CallUnary {
                    func: floor,
                    expr: Box::new(ts.expr.clone()),
                }
                .with_type(ts.typ.clone());
                ret.push((None, floor));

                let ceil = ScalarExpr::CallUnary {
                    func: ceil,
                    expr: Box::new(ts.expr.clone()),
                }
                .with_type(ts.typ.clone());
                ret.push((None, ceil));
            } else {
                ret.push((Some(input_arity + old_idx), expr.clone()))
            }
        }

        // get shuffled index(old_idx -> new_idx)
        // note index is offset by input_arity because mfp is designed to be first include input columns then intermediate columns
        let shuffle = ret
            .iter()
            .map(|(old_idx, _)| *old_idx) // [Option<opt_idx>]
            .enumerate()
            .map(|(new, old)| (old, new + input_arity))
            .flat_map(|(old, new)| old.map(|o| (o, new)))
            .chain((0..input_arity).map(|i| (i, i))) // also remember to chain the input columns as not changed
            .collect::<BTreeMap<_, _>>();

        // shuffle expr's index
        let exprs = ret
            .into_iter()
            .map(|(_, mut expr)| {
                // invariant: it is expect that no expr will try to refer the column being expanded
                expr.expr.permute_map(&shuffle)?;
                Ok(expr)
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(exprs)
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

/// A way to represent a scalar function that is implemented in Datafusion
#[derive(Debug, Clone)]
pub struct DfScalarFunction {
    raw_fn: RawDfScalarFn,
    // TODO(discord9): directly from datafusion expr
    fn_impl: Arc<dyn PhysicalExpr>,
    df_schema: Arc<datafusion_common::DFSchema>,
}

impl DfScalarFunction {
    pub fn new(raw_fn: RawDfScalarFn, fn_impl: Arc<dyn PhysicalExpr>) -> Result<Self, Error> {
        Ok(Self {
            df_schema: Arc::new(raw_fn.input_schema.to_df_schema()?),
            raw_fn,
            fn_impl,
        })
    }

    pub async fn try_from_raw_fn(raw_fn: RawDfScalarFn) -> Result<Self, Error> {
        Ok(Self {
            fn_impl: raw_fn.get_fn_impl().await?,
            df_schema: Arc::new(raw_fn.input_schema.to_df_schema()?),
            raw_fn,
        })
    }

    /// eval a list of expressions using input values
    fn eval_args(values: &[Value], exprs: &[ScalarExpr]) -> Result<Vec<Value>, EvalError> {
        exprs
            .iter()
            .map(|expr| expr.eval(values))
            .collect::<Result<_, _>>()
    }

    // TODO(discord9): add RecordBatch support
    pub fn eval(&self, values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
        // first eval exprs to construct values to feed to datafusion
        let values: Vec<_> = Self::eval_args(values, exprs)?;
        if values.is_empty() {
            return InvalidArgumentSnafu {
                reason: "values is empty".to_string(),
            }
            .fail();
        }
        // TODO(discord9): make cols all array length of one
        let mut cols = vec![];
        for (idx, typ) in self
            .raw_fn
            .input_schema
            .typ()
            .column_types
            .iter()
            .enumerate()
        {
            let typ = typ.scalar_type();
            let mut array = typ.create_mutable_vector(1);
            array.push_value_ref(values[idx].as_value_ref());
            cols.push(array.to_vector().to_arrow_array());
        }
        let schema = self.df_schema.inner().clone();
        let rb = DfRecordBatch::try_new(schema, cols).map_err(|err| {
            ArrowSnafu {
                raw: err,
                context:
                    "Failed to create RecordBatch from values when eval datafusion scalar function",
            }
            .build()
        })?;

        let res = self.fn_impl.evaluate(&rb).map_err(|err| {
            EvalDatafusionSnafu {
                raw: err,
                context: "Failed to evaluate datafusion scalar function",
            }
            .build()
        })?;
        let res = common_query::columnar_value::ColumnarValue::try_from(&res)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let res_vec = res
            .try_into_vector(1)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        let res_val = res_vec
            .try_get(0)
            .map_err(BoxedError::new)
            .context(ExternalSnafu)?;
        Ok(res_val)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RawDfScalarFn {
    /// The raw bytes encoded datafusion scalar function
    pub(crate) f: bytes::BytesMut,
    /// The input schema of the function
    pub(crate) input_schema: RelationDesc,
    /// Extension contains mapping from function reference to function name
    pub(crate) extensions: FunctionExtensions,
}

impl RawDfScalarFn {
    pub fn from_proto(
        f: &substrait::substrait_proto_df::proto::expression::ScalarFunction,
        input_schema: RelationDesc,
        extensions: FunctionExtensions,
    ) -> Result<Self, Error> {
        let mut buf = BytesMut::new();
        f.encode(&mut buf)
            .context(EncodeRelSnafu)
            .map_err(BoxedError::new)
            .context(crate::error::ExternalSnafu)?;
        Ok(Self {
            f: buf,
            input_schema,
            extensions,
        })
    }
    async fn get_fn_impl(&self) -> Result<Arc<dyn PhysicalExpr>, Error> {
        let f = ScalarFunction::decode(&mut self.f.as_ref())
            .context(DecodeRelSnafu)
            .map_err(BoxedError::new)
            .context(crate::error::ExternalSnafu)?;
        debug!("Decoded scalar function: {:?}", f);

        let input_schema = &self.input_schema;
        let extensions = &self.extensions;

        from_scalar_fn_to_df_fn_impl(&f, input_schema, extensions).await
    }
}

impl std::cmp::PartialEq for DfScalarFunction {
    fn eq(&self, other: &Self) -> bool {
        self.raw_fn.eq(&other.raw_fn)
    }
}

// can't derive Eq because of Arc<dyn PhysicalExpr> not eq, so implement it manually
impl std::cmp::Eq for DfScalarFunction {}

impl std::cmp::PartialOrd for DfScalarFunction {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl std::cmp::Ord for DfScalarFunction {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.raw_fn.cmp(&other.raw_fn)
    }
}
impl std::hash::Hash for DfScalarFunction {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.raw_fn.hash(state);
    }
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
    use datatypes::arrow::array::Scalar;
    use query::parser::QueryLanguageParser;
    use query::QueryEngine;
    use session::context::QueryContext;
    use substrait::extension_serializer;
    use substrait::substrait_proto_df::proto::expression::literal::LiteralType;
    use substrait::substrait_proto_df::proto::expression::Literal;
    use substrait::substrait_proto_df::proto::function_argument::ArgType;
    use substrait::substrait_proto_df::proto::r#type::Kind;
    use substrait::substrait_proto_df::proto::{r#type, FunctionArgument, Type};

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

    #[tokio::test]
    async fn test_df_scalar_function() {
        let raw_scalar_func = ScalarFunction {
            function_reference: 0,
            arguments: vec![FunctionArgument {
                arg_type: Some(ArgType::Value(Expression {
                    rex_type: Some(RexType::Literal(Literal {
                        nullable: false,
                        type_variation_reference: 0,
                        literal_type: Some(LiteralType::I64(-1)),
                    })),
                })),
            }],
            output_type: None,
            ..Default::default()
        };
        let input_schema = RelationDesc::try_new(
            RelationType::new(vec![ColumnType::new_nullable(
                ConcreteDataType::null_datatype(),
            )]),
            vec!["null_column".to_string()],
        )
        .unwrap();
        let extensions = FunctionExtensions::from_iter(vec![(0, "abs")]);
        let raw_fn = RawDfScalarFn::from_proto(&raw_scalar_func, input_schema, extensions).unwrap();
        let df_func = DfScalarFunction::try_from_raw_fn(raw_fn).await.unwrap();
        assert_eq!(
            df_func
                .eval(&[Value::Null], &[ScalarExpr::Column(0)])
                .unwrap(),
            Value::Int64(1)
        );
    }
}
