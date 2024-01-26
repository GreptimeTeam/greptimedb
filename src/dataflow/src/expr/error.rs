use std::any::Any;

use common_macro::stack_trace_debug;
use common_telemetry::common_error::ext::ErrorExt;
use common_telemetry::common_error::status_code::StatusCode;
use datatypes::data_type::ConcreteDataType;
use serde::{Deserialize, Serialize};
use snafu::{Location, Snafu};

/// EvalError is about errors happen on columnar evaluation
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
#[derive(Ord, PartialOrd, Clone, Eq, Deserialize, Serialize, PartialEq, Hash)]
pub enum EvalError {
    #[snafu(display("Division by zero"))]
    DivisionByZero,
    #[snafu(display("Type mismatch: expected {}, actual {}", expected, actual))]
    TypeMismatch {
        expected: ConcreteDataType,
        actual: ConcreteDataType,
    },
    /// can't nest datatypes error because EvalError need to be store in map and serialization
    #[snafu(display("Fail to unpack from value to given type: {}", msg))]
    TryFromValue { msg: String },
    #[snafu(display(
        "Fail to cast value of type {} to given type: {}. Detail: {}",
        from,
        to,
        msg
    ))]
    CastValue {
        from: ConcreteDataType,
        to: ConcreteDataType,
        msg: String,
    },
    #[snafu(display("Invalid argument: {}", reason))]
    InvalidArgument { reason: String },
    #[snafu(display("Internal error: {}", reason))]
    Internal { reason: String },
    #[snafu(display("Optimize error: {}", reason))]
    Optimize { reason: String },
}
