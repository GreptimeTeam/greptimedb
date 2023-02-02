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
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::External { source } => source.status_code(),
            Error::ToJson { .. } => StatusCode::Internal,
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
}
