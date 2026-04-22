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

    #[snafu(display("Incomplete snapshot: chunk {} has status {:?}", chunk_id, status))]
    IncompleteSnapshot {
        chunk_id: u32,
        status: ChunkStatus,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Snapshot is inconsistent: chunk {} is marked completed but its file manifest is empty",
        chunk_id
    ))]
    EmptyChunkManifest {
        chunk_id: u32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Snapshot is inconsistent: chunk {} for schema '{}' is marked completed but no files were found under '{}'",
        chunk_id,
        schema,
        path
    ))]
    MissingChunkData {
        chunk_id: u32,
        schema: String,
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Chunk {} import failed for schema '{}'", chunk_id, schema))]
    ChunkImportFailed {
        chunk_id: u32,
        schema: String,
        #[snafu(source)]
        error: crate::data::export_v2::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Snapshot storage error"))]
    SnapshotStorage {
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

    #[snafu(display("Failed to parse import state file"))]
    ImportStateParse {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Import state I/O failed at '{}': {}", path, error))]
    ImportStateIo {
        path: String,
        #[snafu(source)]
        error: std::io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Import state is already locked at '{}'", path))]
    ImportStateLocked {
        path: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Import state references unknown chunk {}", chunk_id))]
    ImportStateUnknownChunk {
        chunk_id: u32,
        #[snafu(implicit)]
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::SnapshotNotFound { .. }
            | Error::SchemaNotInSnapshot { .. }
            | Error::ManifestVersionMismatch { .. }
            | Error::IncompleteSnapshot { .. }
            | Error::EmptyChunkManifest { .. }
            | Error::MissingChunkData { .. } => StatusCode::InvalidArguments,
            Error::ImportStateUnknownChunk { .. } => StatusCode::Internal,
            Error::Database { error, .. } => error.status_code(),
            Error::SnapshotStorage { error, .. } | Error::ChunkImportFailed { error, .. } => {
                error.status_code()
            }
            Error::ImportStateParse { .. } => StatusCode::Internal,
            Error::ImportStateIo { .. } => StatusCode::StorageUnavailable,
            Error::ImportStateLocked { .. } => StatusCode::IllegalState,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
