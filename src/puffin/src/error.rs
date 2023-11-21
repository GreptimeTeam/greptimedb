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

    #[snafu(display("Failed to flush"))]
    Flush {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Failed to close"))]
    Close {
        #[snafu(source)]
        error: IoError,
        location: Location,
    },

    #[snafu(display("Magic not matched"))]
    MagicNotMatched { location: Location },

    #[snafu(display("Failed to convert bytes to integer"))]
    BytesToInteger {
        #[snafu(source)]
        error: std::array::TryFromSliceError,
        location: Location,
    },

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

    #[snafu(display("Parse stage not match, expected: {}, actual: {}", expected, actual))]
    ParseStageNotMatch {
        expected: String,
        actual: String,
        location: Location,
    },

    #[snafu(display("Unexpected footer payload size: {}", size))]
    UnexpectedFooterPayloadSize { size: i32, location: Location },

    #[snafu(display(
        "Unexpected puffin file size, min: {}, actual: {}",
        min_file_size,
        actual_file_size
    ))]
    UnexpectedPuffinFileSize {
        min_file_size: u64,
        actual_file_size: u64,
        location: Location,
    },

    #[snafu(display("Invalid blob offset: {}, location: {:?}", offset, location))]
    InvalidBlobOffset { offset: i64, location: Location },

    #[snafu(display("Invalid blob area end: {}, location: {:?}", offset, location))]
    InvalidBlobAreaEnd { offset: u64, location: Location },
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
            | Flush { .. }
            | Close { .. }
            | SerializeJson { .. }
            | BytesToInteger { .. }
            | ParseStageNotMatch { .. }
            | UnexpectedFooterPayloadSize { .. }
            | UnexpectedPuffinFileSize { .. }
            | InvalidBlobOffset { .. }
            | InvalidBlobAreaEnd { .. } => StatusCode::Unexpected,

            UnsupportedDecompression { .. } => StatusCode::Unsupported,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
