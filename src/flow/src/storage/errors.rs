use serde::{Deserialize, Serialize};

// TODO(discord9): more error types
#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum DataflowError {
    EvalError(Box<EvalError>),
}

impl From<EvalError> for DataflowError {
    fn from(e: EvalError) -> Self {
        DataflowError::EvalError(Box::new(e))
    }
}

#[derive(Ord, PartialOrd, Clone, Debug, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EvalError {
    DivisionByZero,
    InvalidArgument(String),
    Internal(String),
}

#[test]
fn tell_goal() {
    use differential_dataflow::ExchangeData;
    fn a<T: ExchangeData>(_: T) {}
    a(DataflowError::from(EvalError::DivisionByZero));
}
