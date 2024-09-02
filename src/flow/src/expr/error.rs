// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Error handling for expression evaluation.

use arrow_schema::ArrowError;
use common_error::ext::BoxedError;
use common_macro::stack_trace_debug;
use datafusion_common::DataFusionError;
use datatypes::data_type::ConcreteDataType;
use snafu::{Location, Snafu};

fn is_send_sync() {
    fn check<T: Send + Sync>() {}
    check::<EvalError>();
}

/// EvalError is about errors happen on columnar evaluation
///
/// TODO(discord9): add detailed location of column/operator(instead of code) to errors tp help identify related column
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum EvalError {
    #[snafu(display("Division by zero"))]
    DivisionByZero {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Type mismatch: expected {expected}, actual {actual}"))]
    TypeMismatch {
        expected: ConcreteDataType,
        actual: ConcreteDataType,
        #[snafu(implicit)]
        location: Location,
    },

    /// can't nest datatypes error because EvalError need to be store in map and serialization
    #[snafu(display("Fail to unpack from value to given type: {msg}"))]
    TryFromValue {
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Fail to cast value of type {from} to given type {to}"))]
    CastValue {
        from: ConcreteDataType,
        to: ConcreteDataType,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{msg}"))]
    DataType {
        msg: String,
        source: datatypes::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid argument: {reason}"))]
    InvalidArgument {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Internal error: {reason}"))]
    Internal {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Optimize error: {reason}"))]
    Optimize {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Overflowed during evaluation"))]
    Overflow {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Incoming data already expired by {} ms", expired_by))]
    DataAlreadyExpired {
        expired_by: i64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Arrow error: {error:?}, context: {context}"))]
    Arrow {
        #[snafu(source)]
        error: ArrowError,
        context: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("DataFusion error: {error:?}, context: {context}"))]
    Datafusion {
        #[snafu(source)]
        error: DataFusionError,
        context: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },
}
