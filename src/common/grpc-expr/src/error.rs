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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal delete request, reason: {reason}"))]
    IllegalDeleteRequest { reason: String, location: Location },

    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display(
        "Duplicated timestamp column in gRPC requests, exists {}, duplicated: {}",
        exists,
        duplicated
    ))]
    DuplicatedTimestampColumn {
        exists: String,
        duplicated: String,
        location: Location,
    },

    #[snafu(display("Duplicated column name in gRPC requests, name: {}", name,))]
    DuplicatedColumnName { name: String, location: Location },

    #[snafu(display("Missing timestamp column, msg: {}", msg))]
    MissingTimestampColumn { msg: String, location: Location },

    #[snafu(display("Invalid column proto: {}", err_msg))]
    InvalidColumnProto { err_msg: String, location: Location },
    #[snafu(display("Failed to create vector"))]
    CreateVector {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, location: Location },

    #[snafu(display("Invalid column proto definition, column: {}", column))]
    InvalidColumnDef {
        column: String,
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Unexpected values length, reason: {}", reason))]
    UnexpectedValuesLength { reason: String, location: Location },

    #[snafu(display("Unknown location type: {}", location_type))]
    UnknownLocationType {
        location_type: i32,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalDeleteRequest { .. } => StatusCode::InvalidArguments,

            Error::ColumnDataType { .. } => StatusCode::Internal,
            Error::DuplicatedTimestampColumn { .. }
            | Error::DuplicatedColumnName { .. }
            | Error::MissingTimestampColumn { .. } => StatusCode::InvalidArguments,
            Error::InvalidColumnProto { .. } => StatusCode::InvalidArguments,
            Error::CreateVector { .. } => StatusCode::InvalidArguments,
            Error::MissingField { .. } => StatusCode::InvalidArguments,
            Error::InvalidColumnDef { source, .. } => source.status_code(),
            Error::UnexpectedValuesLength { .. } | Error::UnknownLocationType { .. } => {
                StatusCode::InvalidArguments
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
