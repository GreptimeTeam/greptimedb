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

use api::DecodeError;
use common_error::ext::ErrorExt;
use common_error::prelude::{Snafu, StatusCode};
use snafu::Location;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Column `{}` not found in table `{}`", column_name, table_name))]
    ColumnNotFound {
        column_name: String,
        table_name: String,
    },

    #[snafu(display("Failed to convert bytes to insert batch, source: {}", source))]
    DecodeInsert { source: DecodeError },

    #[snafu(display("Illegal delete request, reason: {reason}"))]
    IllegalDeleteRequest { reason: String, location: Location },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
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

    #[snafu(display("Missing timestamp column, msg: {}", msg))]
    MissingTimestampColumn { msg: String, location: Location },

    #[snafu(display("Invalid column proto: {}", err_msg))]
    InvalidColumnProto { err_msg: String, location: Location },
    #[snafu(display("Failed to create vector, source: {}", source))]
    CreateVector {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, location: Location },

    #[snafu(display("Invalid column default constraint, source: {}", source))]
    ColumnDefaultConstraint {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Invalid column proto definition, column: {}, source: {}",
        column,
        source
    ))]
    InvalidColumnDef {
        column: String,
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Unrecognized table option: {}", source))]
    UnrecognizedTableOption {
        #[snafu(backtrace)]
        source: table::error::Error,
    },

    #[snafu(display("Unexpected values length, reason: {}", reason))]
    UnexpectedValuesLength { reason: String, location: Location },

    #[snafu(display("The column name already exists, column: {}", column))]
    ColumnAlreadyExists { column: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::ColumnNotFound { .. } => StatusCode::TableColumnNotFound,

            Error::DecodeInsert { .. } | Error::IllegalDeleteRequest { .. } => {
                StatusCode::InvalidArguments
            }

            Error::ColumnDataType { .. } => StatusCode::Internal,
            Error::DuplicatedTimestampColumn { .. } | Error::MissingTimestampColumn { .. } => {
                StatusCode::InvalidArguments
            }
            Error::InvalidColumnProto { .. } => StatusCode::InvalidArguments,
            Error::CreateVector { .. } => StatusCode::InvalidArguments,
            Error::MissingField { .. } => StatusCode::InvalidArguments,
            Error::ColumnDefaultConstraint { source, .. } => source.status_code(),
            Error::InvalidColumnDef { source, .. } => source.status_code(),
            Error::UnrecognizedTableOption { .. } => StatusCode::InvalidArguments,
            Error::UnexpectedValuesLength { .. } | Error::ColumnAlreadyExists { .. } => {
                StatusCode::InvalidArguments
            }
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
