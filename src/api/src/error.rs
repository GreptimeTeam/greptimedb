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
use datatypes::prelude::ConcreteDataType;
use snafu::prelude::*;
use snafu::Location;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unknown proto column datatype: {}", datatype))]
    UnknownColumnDataType { datatype: i32, location: Location },

    #[snafu(display("Failed to create column datatype from {:?}", from))]
    IntoColumnDataType {
        from: ConcreteDataType,
        location: Location,
    },

    #[snafu(display(
        "Failed to convert column default constraint, column: {}, source: {}",
        column,
        source
    ))]
    ConvertColumnDefaultConstraint {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display(
        "Invalid column default constraint, column: {}, source: {}",
        column,
        source
    ))]
    InvalidColumnDefaultConstraint {
        column: String,
        location: Location,
        source: datatypes::error::Error,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::UnknownColumnDataType { .. } => StatusCode::InvalidArguments,
            Error::IntoColumnDataType { .. } => StatusCode::Unexpected,
            Error::ConvertColumnDefaultConstraint { source, .. }
            | Error::InvalidColumnDefaultConstraint { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
