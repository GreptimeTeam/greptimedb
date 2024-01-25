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

use std::any::Any;

use common_macro::stack_trace_debug;
use common_telemetry::common_error::ext::ErrorExt;
use common_telemetry::common_error::status_code::StatusCode;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use serde::{Deserialize, Serialize};
use servers::define_into_tonic_status;
use snafu::{Location, Snafu};

use crate::expr::EvalError;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    /// TODO(discord9): add detailed location of column
    #[snafu(display("Stream Eval Failure, {}", raw))]
    Eval { raw: EvalError },
    #[snafu(display("Couldn't found table: {}", name))]
    TableNotFound { name: String },
    #[snafu(display("Table already exist: {}", name))]
    TableAlreadyExist { name: String },
    #[snafu(display("Failed to join task"))]
    JoinTask {
        #[snafu(source)]
        error: tokio::task::JoinError,
        location: Location,
    },
    #[snafu(display("Invalid query: {}", reason))]
    InvalidQuery { reason: String, location: Location },
    #[snafu(display("No protobuf type for value: {}", value))]
    NoProtoType { value: Value },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    /// TODO(discord9): more detailed status code
    fn status_code(&self) -> StatusCode {
        match self {
            Self::Eval { .. }
            | &Self::TableAlreadyExist { .. }
            | &Self::JoinTask { .. }
            | &Self::InvalidQuery { .. }
            | &Self::TableNotFound { .. }
            | Self::NoProtoType { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_into_tonic_status!(Error);

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
