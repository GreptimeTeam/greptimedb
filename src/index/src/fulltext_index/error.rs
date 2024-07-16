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
use std::io::Error as IoError;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("IO error"))]
    Io {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Tantivy error"))]
    Tantivy {
        #[snafu(source)]
        error: tantivy::TantivyError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Tantivy parser error"))]
    TantivyParser {
        #[snafu(source)]
        error: tantivy::query::QueryParserError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Operate on a finished creator"))]
    Finished {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;

        match self {
            Tantivy { .. } => StatusCode::Internal,
            TantivyParser { .. } => StatusCode::InvalidSyntax,

            Io { .. } | Finished { .. } => StatusCode::Unexpected,

            External { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
