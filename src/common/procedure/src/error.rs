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

use crate::procedure::ProcedureId;

/// Procedure error.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display(
        "Failed to execute procedure due to external error, source: {}",
        source
    ))]
    External {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Loader {} is already registered", name))]
    LoaderConflict { name: String, backtrace: Backtrace },

    #[snafu(display("Failed to serialize to json, source: {}", source))]
    ToJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Procedure {} already exists", procedure_id))]
    DuplicateProcedure {
        procedure_id: ProcedureId,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to put {}, source: {}", key, source))]
    PutState {
        key: String,
        source: object_store::Error,
    },

    #[snafu(display("Failed to delete {}, source: {}", key, source))]
    DeleteState {
        key: String,
        source: object_store::Error,
    },

    #[snafu(display("Failed to list {}, source: {}", path, source))]
    ListState {
        path: String,
        source: object_store::Error,
    },

    #[snafu(display("Failed to read {}, source: {}", key, source))]
    ReadState {
        key: String,
        source: object_store::Error,
    },

    #[snafu(display("Failed to deserialize from json, source: {}", source))]
    FromJson {
        source: serde_json::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Procedure exec failed, source: {}", source))]
    RetryLater {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::External { source } => source.status_code(),
            Error::ToJson { .. }
            | Error::PutState { .. }
            | Error::DeleteState { .. }
            | Error::ListState { .. }
            | Error::ReadState { .. }
            | Error::FromJson { .. }
            | Error::RetryLater { .. } => StatusCode::Internal,
            Error::LoaderConflict { .. } | Error::DuplicateProcedure { .. } => {
                StatusCode::InvalidArguments
            }
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
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
