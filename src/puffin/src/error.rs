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
use std::sync::Arc;

use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Failed to read"))]
    Read {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to write"))]
    Write {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to flush"))]
    Flush {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to close"))]
    Close {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to open"))]
    Open {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to read metadata"))]
    Metadata {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create"))]
    Create {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to rename"))]
    Rename {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to remove"))]
    Remove {
        #[snafu(source)]
        error: IoError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Error while walking directory"))]
    WalkDir {
        #[snafu(source)]
        error: async_walkdir::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Magic not matched"))]
    MagicNotMatched {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported decompression: {}", decompression))]
    UnsupportedDecompression {
        decompression: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to serialize json"))]
    SerializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to deserialize json"))]
    DeserializeJson {
        #[snafu(source)]
        error: serde_json::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unexpected footer payload size: {}", size))]
    UnexpectedFooterPayloadSize {
        size: i32,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid puffin footer"))]
    InvalidPuffinFooter {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Unexpected puffin file size, min: {}, actual: {}",
        min_file_size,
        actual_file_size
    ))]
    UnexpectedPuffinFileSize {
        min_file_size: u64,
        actual_file_size: u64,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to compress lz4"))]
    Lz4Compression {
        #[snafu(source)]
        error: std::io::Error,

        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decompress lz4"))]
    Lz4Decompression {
        #[snafu(source)]
        error: serde_json::Error,

        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Unsupported compression: {codec}"))]
    UnsupportedCompression {
        codec: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Write to the same blob twice: {blob}"))]
    DuplicateBlob {
        blob: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Blob not found: {blob}"))]
    BlobNotFound {
        blob: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Blob index out of bound, index: {}, max index: {}", index, max_index))]
    BlobIndexOutOfBound {
        index: usize,
        max_index: usize,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("File key not match, expected: {}, actual: {}", expected, actual))]
    FileKeyNotMatch {
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Get value from cache"))]
    CacheGet { source: Arc<Error> },

    #[snafu(display("External error"))]
    External {
        #[snafu(source)]
        error: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        use Error::*;
        match self {
            Read { .. }
            | MagicNotMatched { .. }
            | DeserializeJson { .. }
            | Write { .. }
            | Flush { .. }
            | Close { .. }
            | Open { .. }
            | Metadata { .. }
            | Create { .. }
            | Remove { .. }
            | Rename { .. }
            | SerializeJson { .. }
            | UnexpectedFooterPayloadSize { .. }
            | UnexpectedPuffinFileSize { .. }
            | Lz4Compression { .. }
            | Lz4Decompression { .. }
            | BlobNotFound { .. }
            | BlobIndexOutOfBound { .. }
            | FileKeyNotMatch { .. }
            | WalkDir { .. }
            | InvalidPuffinFooter { .. } => StatusCode::Unexpected,

            UnsupportedCompression { .. } | UnsupportedDecompression { .. } => {
                StatusCode::Unsupported
            }

            DuplicateBlob { .. } => StatusCode::InvalidArguments,

            CacheGet { source } => source.status_code(),

            External { error, .. } => error.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
