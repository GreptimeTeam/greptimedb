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

    #[snafu(display(
        "Invalid environment variable key '{key}': '{target}' is a sequence that cannot be \
         overridden as a whole; address one of its entries with '{target}__<TAG>__<FIELD>'"
    ))]
    TaggedSeqMissingTag {
        key: String,
        target: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Invalid environment variable key '{key}': '{tag}' is a numeric index, but '{target}' \
         is addressed by tag name; use '{target}__<TAG>__<FIELD>' instead, e.g. \
         '{target}__MITO__<FIELD>'"
    ))]
    TaggedSeqNumericTag {
        key: String,
        tag: String,
        target: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Invalid environment variable key '{key}': the entry '{tag}' of '{target}' needs a \
         field to override; use '{target}__<TAG>__<FIELD>' instead"
    ))]
    TaggedSeqMissingField {
        key: String,
        tag: String,
        target: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Invalid environment variable key '{key}': another environment variable sets '{path}' \
         as a whole, so this key cannot set a field below it; set either the whole '{path}' or \
         its fields, not both"
    ))]
    TaggedSeqOverridesConflict {
        key: String,
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "'{path}' cannot be overridden from the environment: its default value is not a table, \
         so a field below it cannot be set"
    ))]
    TaggedSeqNotATable {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "'{key}' is registered as a sequence of tagged tables, but it is not a top-level field \
         of the options; the default entries to merge onto are looked up by literal key"
    ))]
    TaggedSeqNotTopLevel {
        key: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "The default value of '{target}' is not a sequence of single-key tables: {reason}"
    ))]
    TaggedSeqInvalidDefault {
        target: String,
        reason: String,
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
            | Error::InvalidPath { .. }
            | Error::TaggedSeqMissingTag { .. }
            | Error::TaggedSeqNumericTag { .. }
            | Error::TaggedSeqMissingField { .. }
            | Error::TaggedSeqOverridesConflict { .. }
            | Error::TaggedSeqNotATable { .. }
            | Error::TaggedSeqNotTopLevel { .. }
            | Error::TaggedSeqInvalidDefault { .. } => StatusCode::InvalidArguments,
            Error::SerdeJson { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
