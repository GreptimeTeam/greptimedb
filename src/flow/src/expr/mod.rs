//! for declare dataflow description that is the last step before build dataflow

mod func;
mod id;
mod linear;
mod relation;

use std::collections::{BTreeMap, BTreeSet};

use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
pub use id::{GlobalId, Id, LocalId};
pub use linear::{MapFilterProject, SafeMfpPlan};
pub(crate) use relation::{AggregateExpr, AggregateFunc, TableFunc};
use serde::{Deserialize, Serialize};

use crate::expr::func::{BinaryFunc, UnaryFunc, VariadicFunc};
use crate::storage::errors::EvalError;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    Literal(Result<Value, EvalError>, ConcreteDataType),
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
    pub fn eval(&self, values: &[Value]) -> Result<Value, EvalError> {
        match self {
            ScalarExpr::Column(index) => Ok(values[*index].clone()),
            ScalarExpr::Literal(row_res, _ty) => row_res.clone(),
            ScalarExpr::CallUnary { func, expr } => func.eval(values, expr),
            ScalarExpr::CallBinary { func, expr1, expr2 } => func.eval(values, expr1, expr2),
            ScalarExpr::CallVariadic { func, exprs } => func.eval(values, exprs),
            ScalarExpr::If { cond, then, els } => match cond.eval(values) {
                Ok(Value::Boolean(true)) => then.eval(values),
                Ok(Value::Boolean(false)) => els.eval(values),
                _ => Err(EvalError::InvalidArgument(
                    "if condition must be boolean".to_string(),
                )),
            },
        }
    }

    /// Rewrites column indices with their value in `permutation`.
    ///
    /// This method is applicable even when `permutation` is not a
    /// strict permutation, and it only needs to have entries for
    /// each column referenced in `self`.
    pub fn permute(&mut self, permutation: &[usize]) {
        #[allow(deprecated)]
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
        #[allow(deprecated)]
        self.visit_mut_post_nolimit(&mut |e| {
            if let ScalarExpr::Column(old_i) = e {
                *old_i = permutation[old_i];
            }
        });
    }

    pub fn support(&self) -> BTreeSet<usize> {
        let mut support = BTreeSet::new();
        #[allow(deprecated)]
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
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) => (),
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
            ScalarExpr::Column(_) | ScalarExpr::Literal(_, _) => (),
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
