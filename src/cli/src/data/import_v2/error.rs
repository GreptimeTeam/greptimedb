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
use uuid::Uuid;

use crate::data::export_v2::manifest::ChunkStatus;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Snapshot not found at '{}'", uri))]
    SnapshotNotFound {
        uri: String,
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

    #[snafu(display("Schema '{}' not found in snapshot", schema))]
    SchemaNotInSnapshot {
        schema: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute DDL: {}", sql))]
    DdlExecution {
        sql: String,
        #[snafu(source)]
        error: crate::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Export error"))]
    Export {
        #[snafu(source)]
        error: crate::data::export_v2::error::Error,
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

    #[snafu(display("Invalid column definition for table '{}': {}", table, reason))]
    InvalidColumnDefinition {
        table: String,
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Incomplete snapshot: chunk {} has status {:?}", chunk_id, status))]
    IncompleteSnapshot {
        chunk_id: u32,
        status: ChunkStatus,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("State operation '{}' failed at {}", operation, path))]
    StateOperation {
        operation: String,
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse state file: {}", path))]
    StateParse {
        path: String,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize state file: {}", path))]
    StateSerialize {
        path: String,
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "State snapshot mismatch: expected {}, found {}. Clean the local state before resuming",
        expected,
        found
    ))]
    StateSnapshotMismatch {
        expected: Uuid,
        found: Uuid,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "State target mismatch: expected '{}', found '{}'. Use --clean-state to reset local state",
        expected,
        found
    ))]
    StateTargetMismatch {
        expected: String,
        found: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Import incomplete: {} chunk(s) failed, state saved at {}",
        failed_chunks,
        state_path
    ))]
    ImportIncomplete {
        failed_chunks: usize,
        state_path: String,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::SnapshotNotFound { .. } | Error::SchemaNotInSnapshot { .. } => {
                StatusCode::InvalidArguments
            }
            Error::ManifestVersionMismatch { .. } => StatusCode::InvalidArguments,
            Error::DdlExecution { error, .. } | Error::Database { error, .. } => {
                error.status_code()
            }
            Error::Export { error, .. } => error.status_code(),
            Error::StateOperation { .. } => StatusCode::StorageUnavailable,
            Error::StateParse { .. }
            | Error::StateSerialize { .. }
            | Error::StateSnapshotMismatch { .. }
            | Error::StateTargetMismatch { .. }
            | Error::InvalidColumnDefinition { .. }
            | Error::IncompleteSnapshot { .. }
            | Error::ImportIncomplete { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
