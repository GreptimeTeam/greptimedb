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

use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub(crate)))]
pub enum Error {
    #[snafu(display("Failed to serialize procedure to json, source: {}", source))]
    SerializeProcedure {
        source: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize procedure from json, source: {}", source))]
    DeserializeProcedure {
        source: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Invalid raw schema, source: {}", source))]
    InvalidRawSchema {
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to access catalog, source: {}", source))]
    AccessCatalog {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Table {} not found", name))]
    TableNotFound { name: String },

    #[snafu(display("Table already exists: {}", name))]
    TableExists { name: String },

    #[snafu(display("Failed to deregister table: {}", name))]
    DeregisterTable { name: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            DeregisterTable { .. } | SerializeProcedure { .. } | DeserializeProcedure { .. } => {
                StatusCode::Internal
            }
            InvalidRawSchema { source, .. } => source.status_code(),
            AccessCatalog { source, .. } => source.status_code(),
            TableNotFound { .. } => StatusCode::TableNotFound,
            TableExists { .. } => StatusCode::TableAlreadyExists,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Error> for common_procedure::Error {
    fn from(e: Error) -> common_procedure::Error {
        common_procedure::Error::from_error_ext(e)
    }
}
