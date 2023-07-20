use serde::{Deserialize, Serialize};

// TODO: more function & eval
use crate::repr::Row;
/// Stateless functions
#[derive(Debug, Clone)]
pub enum Func {
    BuiltIn(BuiltInFunc),
    /// still a strict Row-to-Row function
    Custom(fn(Row) -> Row),
}

#[derive(Debug, Clone)]
pub enum BuiltInFunc {
    Not,
    IsNull,
    IsTrue,
    IsFalse,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunc {
    Count,
    Any,
    All,
}
