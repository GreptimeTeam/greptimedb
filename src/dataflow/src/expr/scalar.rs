use std::collections::{BTreeMap, BTreeSet};

use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use crate::expr::error::{EvalError, InvalidArgumentSnafu, OptimizeSnafu};
use crate::expr::func::{BinaryFunc, UnaryFunc, UnmaterializableFunc, VariadicFunc};

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    /// Extra type info to know original type even when it is null
    Literal(Result<Value, EvalError>, ConcreteDataType),
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
    pub fn call_unary(self, func: UnaryFunc) -> Self {
        ScalarExpr::CallUnary {
            func,
            expr: Box::new(self),
        }
    }

    pub fn call_binary(self, other: Self, func: BinaryFunc) -> Self {
        ScalarExpr::CallBinary {
            func,
            expr1: Box::new(self),
            expr2: Box::new(other),
        }
    }

    pub fn eval(&self, values: &[Value]) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => Ok(values[*index].clone()),
            ScalarExpr::Literal(row_res, _ty) => row_res.clone(),
            ScalarExpr::CallUnmaterializable(f) => OptimizeSnafu {
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
        }
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[*old_i];
            }
        });
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute_map(&mut self, permutation: &BTreeMap<usize, usize>) {
        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        self.visit_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(i) = e {
                support.insert(*i);
            }
        });
        support
    }

    pub fn as_literal(&self) -> Option<Result<Value, &EvalError>> {
        if let ScalarExpr::Literal(lit, _column_type) = self {
            Some(lit.as_ref().map(|row| row.clone()))
        } else {
            None
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, ScalarExpr::Literal(_, _))
    }

    pub fn is_literal_true(&self) -> bool {
        Some(Ok(Value::Boolean(true))) == self.as_literal()
    }

    pub fn is_literal_false(&self) -> bool {
        Some(Ok(Value::Boolean(false))) == self.as_literal()
    }

    pub fn is_literal_null(&self) -> bool {
        Some(Ok(Value::Null)) == self.as_literal()
    }

    pub fn is_literal_ok(&self) -> bool {
        matches!(self, ScalarExpr::Literal(Ok(_), _typ))
    }

    pub fn is_literal_err(&self) -> bool {
        matches!(self, ScalarExpr::Literal(Err(_), _typ))
    }

    pub fn literal_null() -> Self {
        ScalarExpr::Literal(Ok(Value::Null), ConcreteDataType::null_datatype())
    }

    pub fn literal(res: Result<Value, EvalError>, typ: ConcreteDataType) -> Self {
        ScalarExpr::Literal(res, typ)
    }

    pub fn literal_ok(val: Value, typ: ConcreteDataType) -> Self {
        ScalarExpr::Literal(Ok(val), typ)
    }

    pub fn literal_false() -> Self {
        ScalarExpr::Literal(
            Ok(Value::Boolean(false)),
            ConcreteDataType::boolean_datatype(),
        )
    }

    pub fn literal_true() -> Self {
        ScalarExpr::Literal(
            Ok(Value::Boolean(true)),
            ConcreteDataType::boolean_datatype(),
        )
    }
}

impl ScalarExpr {
    /// visit post-order without stack call limit, but may cause stack overflow
    fn visit_post_nolimit<F>(&self, f: &mut F)
    where
        F: FnMut(&Self),
    {
        self.visit_children(|e| e.visit_post_nolimit(f));
        f(self);
    }

    fn visit_children<F>(&self, mut f: F)
    where
        F: FnMut(&Self),
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => (),
            ScalarExpr::CallUnary { func: _, expr } => f(expr),
            ScalarExpr::CallBinary {
                func: as_any,
                expr1,
                expr2,
            } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { func: _, exprs } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
            }
        }
    }

    fn visit_mut_post_nolimit<F>(&mut self, f: &mut F)
    where
        F: FnMut(&mut Self),
    {
        self.visit_mut_children(|e: &mut Self| e.visit_mut_post_nolimit(f));
        f(self);
    }

    fn visit_mut_children<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut Self),
    {
        match self {
            ScalarExpr::Column(_)
            | ScalarExpr::Literal(_, _)
            | ScalarExpr::CallUnmaterializable(_) => (),
            ScalarExpr::CallUnary { func, expr } => f(expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => {
                f(expr1);
                f(expr2);
            }
            ScalarExpr::CallVariadic { func, exprs } => {
                for expr in exprs {
                    f(expr);
                }
            }
            ScalarExpr::If { cond, then, els } => {
                f(cond);
                f(then);
                f(els);
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
        });
        contains
    }

    /// extract lower or upper bound of `Now` for expr,
    ///
    /// returned bool indicates whether the bound is upper bound:
    ///
    /// false for lower bound, true for upper bound
    /// TODO(discord9): allow simple transform like `now() + a < b` to `now() < b - a`
    pub fn extract_bound(&self) -> Result<(Option<Self>, Option<Self>), String> {
        let unsupported_err = || {
            Err(format!(
                "Unsupported temporal predicate. Use `now()` in direct comparison: {:?}",
                self
            ))
        };
        if let Self::CallBinary {
            mut func,
            mut expr1,
            mut expr2,
        } = self.clone()
        {
            if expr1.contains_temporal() ^ expr2.contains_temporal() {
                return unsupported_err();
            }
            if !expr1.contains_temporal()
                && *expr2 == ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)
            {
                std::mem::swap(&mut expr1, &mut expr2);
                func = match func {
                    BinaryFunc::Eq => BinaryFunc::Eq,
                    BinaryFunc::NotEq => BinaryFunc::NotEq,
                    BinaryFunc::Lt => BinaryFunc::Gt,
                    BinaryFunc::Lte => BinaryFunc::Gte,
                    BinaryFunc::Gt => BinaryFunc::Lt,
                    BinaryFunc::Gte => BinaryFunc::Lte,
                    _ => {
                        return unsupported_err();
                    }
                };
            }
            // TODO: support simple transform like `now() + a < b` to `now() < b - a`
            if expr2.contains_temporal()
                || *expr1 != ScalarExpr::CallUnmaterializable(UnmaterializableFunc::Now)
            {
                return unsupported_err();
            }
            let step = |expr: ScalarExpr| expr.call_unary(UnaryFunc::StepTimestamp);
            match func {
                BinaryFunc::Eq => Ok((Some(*expr2.clone()), Some(step(*expr2)))),
                BinaryFunc::Lt => Ok((None, Some(*expr2))),
                BinaryFunc::Lte => Ok((None, Some(step(*expr2)))),
                BinaryFunc::Gt => Ok((Some(step(*expr2)), None)),
                BinaryFunc::Gte => Ok((Some(*expr2), None)),
                _ => unsupported_err(),
            }
        } else {
            unsupported_err()
        }
    }
}
