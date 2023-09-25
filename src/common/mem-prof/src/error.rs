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

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::Snafu;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Internal error"))]
    Internal { source: BoxedError },

    #[snafu(display("Memory profiling is not supported"))]
    ProfilingNotSupported,
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::Internal { source } => source.status_code(),
            Error::ProfilingNotSupported => StatusCode::Unsupported,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
