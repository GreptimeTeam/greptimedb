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
use std::string::FromUtf8Error;
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::procedure::ProcedureId;

/// Procedure error.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to execute procedure due to external error"))]
    External { source: BoxedError },

    #[snafu(display("Loader {} is already registered", name))]
    LoaderConflict { name: String, location: Location },

    #[snafu(display("Procedure Manager is stopped"))]
    ManagerNotStart { location: Location },

    #[snafu(display("Failed to serialize to json"))]
    ToJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Procedure {} already exists", procedure_id))]
    DuplicateProcedure {
        procedure_id: ProcedureId,
        location: Location,
    },

    #[snafu(display("Failed to put state, key: '{key}'"))]
    PutState {
        key: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to delete {}", key))]
    DeleteState {
        key: String,
        #[snafu(source)]
        error: object_store::Error,
    },

    #[snafu(display("Failed to delete keys: '{keys}'"))]
    DeleteStates {
        keys: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list state, path: '{path}'"))]
    ListState {
        path: String,
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to deserialize from json"))]
    FromJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Procedure exec failed"))]
    RetryLater { source: BoxedError },

    #[snafu(display("Procedure panics, procedure_id: {}", procedure_id))]
    ProcedurePanic { procedure_id: ProcedureId },

    #[snafu(display("Failed to wait watcher"))]
    WaitWatcher {
        #[snafu(source)]
        error: tokio::sync::watch::error::RecvError,
        location: Location,
    },

    #[snafu(display("Failed to execute procedure"))]
    ProcedureExec {
        source: Arc<Error>,
        location: Location,
    },

    #[snafu(display("Procedure retry exceeded max times, procedure_id: {}", procedure_id))]
    RetryTimesExceeded {
        source: Arc<Error>,
        procedure_id: ProcedureId,
    },

    #[snafu(display("Corrupted data, error: "))]
    CorruptedData {
        #[snafu(source)]
        error: FromUtf8Error,
    },

    #[snafu(display("Failed to start the remove_outdated_meta method, error"))]
    StartRemoveOutdatedMetaTask {
        source: common_runtime::error::Error,
        location: Location,
    },

    #[snafu(display("Failed to stop the remove_outdated_meta method, error"))]
    StopRemoveOutdatedMetaTask {
        source: common_runtime::error::Error,
        location: Location,
    },

    #[snafu(display("Subprocedure {} failed", subprocedure_id))]
    SubprocedureFailed {
        subprocedure_id: ProcedureId,
        source: Arc<Error>,
        location: Location,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::External { source, .. }
            | Error::PutState { source, .. }
            | Error::DeleteStates { source, .. }
            | Error::ListState { source, .. } => source.status_code(),

            Error::ToJson { .. }
            | Error::DeleteState { .. }
            | Error::FromJson { .. }
            | Error::RetryTimesExceeded { .. }
            | Error::RetryLater { .. }
            | Error::WaitWatcher { .. }
            | Error::ManagerNotStart { .. } => StatusCode::Internal,
            Error::LoaderConflict { .. } | Error::DuplicateProcedure { .. } => {
                StatusCode::InvalidArguments
            }
            Error::ProcedurePanic { .. } | Error::CorruptedData { .. } => StatusCode::Unexpected,
            Error::ProcedureExec { source, .. } => source.status_code(),
            Error::StartRemoveOutdatedMetaTask { source, .. }
            | Error::StopRemoveOutdatedMetaTask { source, .. } => source.status_code(),

            Error::SubprocedureFailed { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Error {
    /// Creates a new [Error::External] error from source `err`.
    pub fn external<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::External {
            source: BoxedError::new(err),
        }
    }

    /// Creates a new [Error::RetryLater] error from source `err`.
    pub fn retry_later<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::RetryLater {
            source: BoxedError::new(err),
        }
    }

    /// Determine whether it is a retry later type through [StatusCode]
    pub fn is_retry_later(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
    }

    /// Creates a new [Error::RetryLater] or [Error::External] error from source `err` according
    /// to its [StatusCode].
    pub fn from_error_ext<E: ErrorExt + Send + Sync + 'static>(err: E) -> Self {
        if err.status_code().is_retryable() {
            Error::retry_later(err)
        } else {
            Error::external(err)
        }
    }
}
