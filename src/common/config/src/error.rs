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
use config::ConfigError;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to load layered config"))]
    LoadLayeredConfig {
        #[snafu(source)]
        error: ConfigError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serde json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize options to TOML"))]
    TomlFormat {
        #[snafu(source)]
        error: toml::ser::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to watch file: {}", path))]
    FileWatch {
        path: String,
        #[snafu(source)]
        error: notify::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid path '{}': expected a file, not a directory", path))]
    InvalidPath {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::TomlFormat { .. }
            | Error::LoadLayeredConfig { .. }
            | Error::FileWatch { .. }
            | Error::InvalidPath { .. } => StatusCode::InvalidArguments,
            Error::SerdeJson { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
