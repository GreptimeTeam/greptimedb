use datatypes::value::Value;
use serde::{Deserialize, Serialize};

use super::ScalarExpr;
// TODO(discord9): more function & eval
use crate::{repr::Row, storage::errors::EvalError};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum UnaryFunc {
    Not,
    IsNull,
    IsTrue,
    IsFalse,
}

impl UnaryFunc {
    pub fn eval(&self, values: &[Value], expr: &ScalarExpr) -> Result<Value, EvalError> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum BinaryFunc {}

impl BinaryFunc {
    pub fn eval(
        &self,
        values: &[Value],
        expr1: &ScalarExpr,
        expr2: &ScalarExpr,
    ) -> Result<Value, EvalError> {
        todo!()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize)]
pub enum VariadicFunc {}

impl VariadicFunc {
    pub fn eval(&self, values: &[Value], exprs: &[ScalarExpr]) -> Result<Value, EvalError> {
        todo!()
    }
}
