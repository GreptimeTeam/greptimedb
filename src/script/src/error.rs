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
use common_error::prelude::{Snafu, StatusCode};
use snafu::Location;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to find scripts table, source: {}", source))]
    FindScriptsTable {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Failed to find column in scripts table, name: {}", name))]
    FindColumnInScriptsTable { name: String, location: Location },

    #[snafu(display("Failed to register scripts table, source: {}", source))]
    RegisterScriptsTable {
        location: Location,
        source: catalog::error::Error,
    },

    #[snafu(display("Scripts table not found"))]
    ScriptsTableNotFound { location: Location },

    #[snafu(display(
        "Failed to insert script to scripts table, name: {}, source: {}",
        name,
        source
    ))]
    InsertScript {
        name: String,
        location: Location,
        source: table::error::Error,
    },

    #[snafu(display("Failed to compile python script, name: {}, source: {}", name, source))]
    CompilePython {
        name: String,
        location: Location,
        source: crate::python::error::Error,
    },

    #[snafu(display("Failed to execute python script {}, source: {}", name, source))]
    ExecutePython {
        name: String,
        location: Location,
        source: crate::python::error::Error,
    },

    #[snafu(display("Script not found, name: {}", name))]
    ScriptNotFound { location: Location, name: String },

    #[snafu(display("Failed to find script by name: {}", name))]
    FindScript {
        name: String,
        location: Location,
        source: query::error::Error,
    },

    #[snafu(display("Failed to collect record batch, source: {}", source))]
    CollectRecords {
        location: Location,
        source: common_recordbatch::error::Error,
    },

    #[snafu(display("Failed to cast type, msg: {}", msg))]
    CastType { msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            FindColumnInScriptsTable { .. } | CastType { .. } => StatusCode::Unexpected,
            ScriptsTableNotFound { .. } => StatusCode::TableNotFound,
            RegisterScriptsTable { source, .. } | FindScriptsTable { source, .. } => {
                source.status_code()
            }
            InsertScript { source, .. } => source.status_code(),
            CompilePython { source, .. } | ExecutePython { source, .. } => source.status_code(),
            FindScript { source, .. } => source.status_code(),
            CollectRecords { source, .. } => source.status_code(),
            ScriptNotFound { .. } => StatusCode::InvalidArguments,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
