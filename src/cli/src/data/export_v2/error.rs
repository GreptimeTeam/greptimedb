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
    #[snafu(display("Invalid URI '{}': {}", uri, reason))]
    InvalidUri {
        uri: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported storage scheme: {}", scheme))]
    UnsupportedScheme {
        scheme: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Storage operation '{}' failed", operation))]
    StorageOperation {
        operation: String,
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse manifest"))]
    ManifestParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize manifest"))]
    ManifestSerialize {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode text file as UTF-8"))]
    TextDecode {
        #[snafu(source)]
        error: std::string::FromUtf8Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Cannot resume snapshot with a different schema_only mode (existing: {}, requested: {}). Use --force to recreate.",
        existing_schema_only,
        requested_schema_only
    ))]
    SchemaOnlyModeMismatch {
        existing_schema_only: bool,
        requested_schema_only: bool,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Cannot resume snapshot with different {} (existing: {}, requested: {}). Use --force to recreate.",
        field,
        existing,
        requested
    ))]
    ResumeConfigMismatch {
        field: String,
        existing: String,
        requested: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse time: invalid format: {}", input))]
    TimeParseInvalidFormat {
        input: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse time: end_time is before start_time"))]
    TimeParseEndBeforeStart {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "chunk_time_window requires both --start-time and --end-time to be specified"
    ))]
    ChunkTimeWindowRequiresBounds {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("--schema-only cannot be used with data export arguments: {}", args))]
    SchemaOnlyArgsNotAllowed {
        args: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Empty result from query"))]
    EmptyResult {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected value type in query result"))]
    UnexpectedValueType {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Database error"))]
    Database {
        #[snafu(source)]
        error: crate::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Snapshot not found at '{}'", uri))]
    SnapshotNotFound {
        uri: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Schema '{}' not found in catalog '{}'", schema, catalog))]
    SchemaNotFound {
        catalog: String,
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse URL"))]
    UrlParse {
        #[snafu(source)]
        error: url::ParseError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build object store"))]
    BuildObjectStore {
        #[snafu(source)]
        error: object_store::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Manifest version mismatch: expected {}, found {}", expected, found))]
    ManifestVersionMismatch {
        expected: u32,
        found: u32,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidUri { .. }
            | Error::UnsupportedScheme { .. }
            | Error::SchemaOnlyModeMismatch { .. }
            | Error::ResumeConfigMismatch { .. }
            | Error::ManifestVersionMismatch { .. }
            | Error::SchemaOnlyArgsNotAllowed { .. } => StatusCode::InvalidArguments,
            Error::TimeParseInvalidFormat { .. }
            | Error::TimeParseEndBeforeStart { .. }
            | Error::ChunkTimeWindowRequiresBounds { .. } => StatusCode::InvalidArguments,

            Error::StorageOperation { .. }
            | Error::ManifestParse { .. }
            | Error::ManifestSerialize { .. }
            | Error::TextDecode { .. }
            | Error::BuildObjectStore { .. } => StatusCode::StorageUnavailable,

            Error::EmptyResult { .. }
            | Error::UnexpectedValueType { .. }
            | Error::UrlParse { .. } => StatusCode::Internal,

            Error::Database { error, .. } => error.status_code(),

            Error::SnapshotNotFound { .. } => StatusCode::InvalidArguments,
            Error::SchemaNotFound { .. } => StatusCode::DatabaseNotFound,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
