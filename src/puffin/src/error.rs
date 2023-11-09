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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to seek"))]
    Seek {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to read"))]
    Read {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to write"))]
    Write {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Magic not matched"))]
    MagicNotMatched { location: Location },

    #[snafu(display("Unsupported decompression: {}", decompression))]
    UnsupportedDecompression {
        decompression: String,
        location: Location,
    },

    #[snafu(display("Failed to serialize json"))]
    SerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },

    #[snafu(display("Failed to deserialize json"))]
    DeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Seek { .. }
            | Read { .. }
            | MagicNotMatched { .. }
            | DeserializeJson { .. }
            | Write { .. }
            | SerializeJson { .. } => StatusCode::Unexpected,

            UnsupportedDecompression { .. } => StatusCode::Unsupported,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
