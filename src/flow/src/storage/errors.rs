use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum DataflowError {
    EvalError(EvalError),
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EvalError {
    DivisionByZero,
}
