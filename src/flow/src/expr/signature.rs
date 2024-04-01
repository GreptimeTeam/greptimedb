use datatypes::data_type::ConcreteDataType;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub struct Signature {
    pub input: SmallVec<[ConcreteDataType; 2]>,
    pub output: ConcreteDataType,
    pub generic_fn: GenericFn,
}

/// Generic function category
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Deserialize, Serialize, Hash)]
pub enum GenericFn {
    // aggregate func
    Max,
    Min,
    Sum,
    Count,
    Any,
    All,
    // unary func
    Not,
    IsNull,
    IsTrue,
    IsFalse,
    StepTimestamp,
    Cast,
    // binary func
    Eq,
    NotEq,
    Lt,
    Lte,
    Gt,
    Gte,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    // varadic func
    And,
    Or,
    // unmaterized func
    Now,
    CurrentSchema,
}
