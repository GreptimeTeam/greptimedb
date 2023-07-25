use serde::{Deserialize, Serialize};

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum DataflowError {
    EvalError(EvalError),
}

impl From<EvalError> for DataflowError {
    fn from(e: EvalError) -> Self {
        DataflowError::EvalError(e)
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EvalError {
    DivisionByZero,
}
