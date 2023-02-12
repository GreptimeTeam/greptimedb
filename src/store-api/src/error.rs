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
use common_error::prelude::{Snafu, StatusCode};
use snafu::{Backtrace, ErrorCompat};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to push data to batch builder, source: {}", source))]
    PushBatch {
        #[snafu(backtrace)]
        source: datatypes::error::Error,
    },

    #[snafu(display("Failed to build batch, {}", msg))]
    BuildBatch { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to read batch, source: {}", source))]
    ReadBatch {
        #[snafu(backtrace)]
        source: BoxedError,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::PushBatch { source } => source.status_code(),
            Error::BuildBatch { .. } => StatusCode::InvalidArguments,
            Error::ReadBatch { source, .. } => source.status_code(),
        }
    }
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
