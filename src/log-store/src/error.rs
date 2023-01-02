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

use common_error::ext::BoxedError;
use common_error::prelude::{ErrorExt, Snafu};
use snafu::{Backtrace, ErrorCompat};
use tokio::task::JoinError;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Failed to encode entry, source: {}", source))]
    Encode { source: common_base::buffer::Error },

    #[snafu(display("Failed to decode entry, remain size: {}", size))]
    Decode { size: usize, backtrace: Backtrace },

    #[snafu(display("No enough data to decode, try again"))]
    DecodeAgain,

    #[snafu(display("Failed to append entry, source: {}", source))]
    Append {
        #[snafu(backtrace)]
        source: BoxedError,
    },

    #[snafu(display("Failed to wait for log file write complete, source: {}", source))]
    Write { source: tokio::task::JoinError },

    #[snafu(display("Entry corrupted, msg: {}", msg))]
    Corrupted { msg: String, backtrace: Backtrace },

    #[snafu(display("IO error, source: {}", source))]
    Io {
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to create path {}, source: {}", path, source))]
    CreateDir {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to read path {}, source: {}", path, source))]
    ReadPath {
        path: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to open log file {}, source: {}", file_name, source))]
    OpenLog {
        file_name: String,
        source: std::io::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("File name {} illegal", file_name))]
    FileNameIllegal {
        file_name: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Internal error, msg: {}", msg))]
    Internal { msg: String, backtrace: Backtrace },

    #[snafu(display("End of LogFile"))]
    Eof,

    #[snafu(display("File duplicate on start: {}", msg))]
    DuplicateFile { msg: String },

    #[snafu(display("Log file suffix is illegal: {}", suffix))]
    SuffixIllegal { suffix: String },

    #[snafu(display("Failed while waiting for write to finish, source: {}", source))]
    WaitWrite { source: tokio::task::JoinError },

    #[snafu(display("Invalid logstore status, msg: {}", msg))]
    InvalidState { msg: String, backtrace: Backtrace },

    #[snafu(display("Failed to wait for gc task to stop, source: {}", source))]
    WaitGcTaskStop {
        source: JoinError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to encode entry to protobuf, source: {}", source))]
    EncodeProtobuf {
        source: protobuf::error::ProtobufError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to decode entry to protobuf bytes, source: {}", source))]
    DecodeProtobuf {
        source: protobuf::error::ProtobufError,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to add entry to LogBatch, source: {}", source))]
    AddEntryLogBatch {
        source: raft_engine::Error,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to perform raft-engine operation, source: {}", source))]
    RaftEngine {
        source: raft_engine::Error,
        backtrace: Backtrace,
    },
}

impl ErrorExt for Error {
    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub type Result<T> = std::result::Result<T, Error>;
