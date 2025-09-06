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
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

use crate::PoisonKey;
use crate::procedure::ProcedureId;

/// Procedure error.
#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to check procedure manager status"))]
    CheckStatus {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Manager is pasued"))]
    ManagerPasued {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Failed to execute procedure due to external error, clean poisons: {}",
        clean_poisons
    ))]
    External {
        source: BoxedError,
        clean_poisons: bool,
    },

    #[snafu(display("Loader {} is already registered", name))]
    LoaderConflict {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Procedure Manager is stopped"))]
    ManagerNotStart {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize to json"))]
    ToJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Procedure {} already exists", procedure_id))]
    DuplicateProcedure {
        procedure_id: ProcedureId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Too many running procedures, max: {}", max_running_procedures))]
    TooManyRunningProcedures {
        max_running_procedures: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to put state, key: '{key}'"))]
    PutState {
        key: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to put poison, key: '{key}', token: '{token}'"))]
    PutPoison {
        key: String,
        token: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to get poison, key: '{key}'"))]
    GetPoison {
        key: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to delete poison, key: '{key}', token: '{token}'"))]
    DeletePoison {
        key: String,
        token: String,
        #[snafu(implicit)]
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
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to list state, path: '{path}'"))]
    ListState {
        path: String,
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to deserialize from json"))]
    FromJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Procedure exec failed"))]
    RetryLater {
        source: BoxedError,
        clean_poisons: bool,
    },

    #[snafu(display("Procedure panics, procedure_id: {}", procedure_id))]
    ProcedurePanic { procedure_id: ProcedureId },

    #[snafu(display("Failed to wait watcher"))]
    WaitWatcher {
        #[snafu(source)]
        error: tokio::sync::watch::error::RecvError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to execute procedure"))]
    ProcedureExec {
        source: Arc<Error>,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Rollback Procedure recovered: {error}"))]
    RollbackProcedureRecovered {
        error: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Procedure retry exceeded max times, procedure_id: {}", procedure_id))]
    RetryTimesExceeded {
        source: Arc<Error>,
        procedure_id: ProcedureId,
    },

    #[snafu(display(
        "Procedure rollback exceeded max times, procedure_id: {}",
        procedure_id
    ))]
    RollbackTimesExceeded {
        source: Arc<Error>,
        procedure_id: ProcedureId,
    },

    #[snafu(display("Failed to start the remove_outdated_meta method, error"))]
    StartRemoveOutdatedMetaTask {
        source: common_runtime::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to stop the remove_outdated_meta method, error"))]
    StopRemoveOutdatedMetaTask {
        source: common_runtime::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to parse segment key: {key}"))]
    ParseSegmentKey {
        #[snafu(implicit)]
        location: Location,
        key: String,
        #[snafu(source)]
        error: std::num::ParseIntError,
    },

    #[snafu(display("Unexpected: {err_msg}"))]
    Unexpected {
        #[snafu(implicit)]
        location: Location,
        err_msg: String,
    },

    #[snafu(display("Not support to rollback the procedure"))]
    RollbackNotSupported {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Procedure not found, procedure_id: {}", procedure_id))]
    ProcedureNotFound {
        procedure_id: ProcedureId,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Poison key not defined, key: '{key}', procedure_id: '{procedure_id}'"))]
    PoisonKeyNotDefined {
        key: PoisonKey,
        procedure_id: ProcedureId,
        #[snafu(implicit)]
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
            | Error::ListState { source, .. }
            | Error::PutPoison { source, .. }
            | Error::DeletePoison { source, .. }
            | Error::GetPoison { source, .. }
            | Error::CheckStatus { source, .. }
            | Error::RetryLater { source, .. } => source.status_code(),

            Error::ToJson { .. }
            | Error::DeleteState { .. }
            | Error::FromJson { .. }
            | Error::WaitWatcher { .. } => StatusCode::Internal,

            Error::RetryTimesExceeded { .. }
            | Error::RollbackTimesExceeded { .. }
            | Error::ManagerNotStart { .. }
            | Error::ManagerPasued { .. }
            | Error::TooManyRunningProcedures { .. }
            | Error::RollbackProcedureRecovered { .. } => StatusCode::IllegalState,

            Error::RollbackNotSupported { .. } => StatusCode::Unsupported,
            Error::LoaderConflict { .. } | Error::DuplicateProcedure { .. } => {
                StatusCode::InvalidArguments
            }
            Error::ProcedurePanic { .. }
            | Error::ParseSegmentKey { .. }
            | Error::Unexpected { .. }
            | &Error::ProcedureNotFound { .. }
            | Error::PoisonKeyNotDefined { .. } => StatusCode::Unexpected,
            Error::ProcedureExec { source, .. } => source.status_code(),
            Error::StartRemoveOutdatedMetaTask { source, .. }
            | Error::StopRemoveOutdatedMetaTask { source, .. } => source.status_code(),
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
            clean_poisons: false,
        }
    }

    /// Creates a new [Error::External] error from source `err` and clean poisons.
    pub fn external_and_clean_poisons<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::External {
            source: BoxedError::new(err),
            clean_poisons: true,
        }
    }

    /// Creates a new [Error::RetryLater] error from source `err`.
    pub fn retry_later<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::RetryLater {
            source: BoxedError::new(err),
            clean_poisons: false,
        }
    }

    /// Creates a new [Error::RetryLater] error from source `err` and clean poisons.
    pub fn retry_later_and_clean_poisons<E: ErrorExt + Send + Sync + 'static>(err: E) -> Error {
        Error::RetryLater {
            source: BoxedError::new(err),
            clean_poisons: true,
        }
    }

    /// Determine whether it is a retry later type through [StatusCode]
    pub fn is_retry_later(&self) -> bool {
        matches!(self, Error::RetryLater { .. })
    }

    /// Determine whether it needs to clean poisons.
    pub fn need_clean_poisons(&self) -> bool {
        matches!(self, Error::External { clean_poisons, .. } if *clean_poisons)
            || matches!(self, Error::RetryLater { clean_poisons, .. } if *clean_poisons)
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
