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
use url::ParseError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Unsupported backend protocol: {}", protocol))]
    UnsupportedBackendProtocol { protocol: String },

    #[snafu(display("empty host: {}", url))]
    EmptyHostPath { url: String },

    #[snafu(display("Invalid path: {}", path))]
    InvalidPath { path: String },

    #[snafu(display("Invalid url: {}, error :{}", url, source))]
    InvalidUrl { url: String, source: ParseError },

    #[snafu(display("Failed to build backend, source: {}", source))]
    BuildBackend {
        source: object_store::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to list object in path: {}, source: {}", path, source))]
    ListObjects {
        path: String,
        backtrace: Backtrace,
        source: object_store::Error,
    },

    #[snafu(display("Invalid connection: {}", msg))]
    InvalidConnection { msg: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            BuildBackend { .. } | ListObjects { .. } => StatusCode::StorageUnavailable,

            UnsupportedBackendProtocol { .. }
            | InvalidConnection { .. }
            | InvalidUrl { .. }
            | EmptyHostPath { .. }
            | InvalidPath { .. } => StatusCode::InvalidArguments,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
