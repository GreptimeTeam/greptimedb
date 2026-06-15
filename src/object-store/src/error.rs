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

use common_macro::stack_trace_debug;
use common_telemetry::common_error::ext::{ErrorExt, RetryHint};
use common_telemetry::common_error::status_code::StatusCode;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to init backend"))]
    InitBackend {
        #[snafu(source)]
        error: opendal::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to build http client"))]
    BuildHttpClient {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: reqwest::Error,
    },

    #[snafu(display("Failed to create directory {}", dir))]
    CreateDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },

    #[snafu(display("Failed to remove directory {}", dir))]
    RemoveDir {
        dir: String,
        #[snafu(source)]
        error: std::io::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Converts an `opendal::Error` into a `RetryHint`.
pub fn retry_hint_from_opendal_error(error: &opendal::Error) -> RetryHint {
    if error.is_temporary() {
        RetryHint::Retryable
    } else {
        RetryHint::NonRetryable
    }
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            InitBackend { .. } => StatusCode::StorageUnavailable,
            BuildHttpClient { .. } => StatusCode::Unexpected,
            CreateDir { .. } | RemoveDir { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn retry_hint(&self) -> RetryHint {
        match self {
            Error::InitBackend { error, .. } => retry_hint_from_opendal_error(error),
            _ => RetryHint::NonRetryable,
        }
    }
}
