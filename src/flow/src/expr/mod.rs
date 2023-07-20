//! for declare dataflow description that is the last step before build dataflow

mod func;
mod id;

use datatypes::prelude::ConcreteDataType;
use datatypes::value::Value;
pub use id::{GlobalId, Id, LocalId};
use serde::{Deserialize, Serialize};

use crate::storage::errors::DataflowError;

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub enum ScalarExpr {
    /// A column of the input row
    Column(usize),
    /// A literal value.
    Literal(Result<Value, DataflowError>, ConcreteDataType),
    CallFunc {
        func: String,
        exprs: Vec<ScalarExpr>,
    },
}
