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
use std::io;

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use datatypes::arrow::error::ArrowError;
use snafu::{Location, Snafu};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Invalid client tls config, {}", msg))]
    InvalidTlsConfig { msg: String },

    #[snafu(display("Invalid config file path"))]
    InvalidConfigFilePath {
        #[snafu(source)]
        error: io::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to watch config file path: {}", path))]
    FileWatch {
        path: String,
        #[snafu(source)]
        error: notify::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display(
        "Write type mismatch, column name: {}, expected: {}, actual: {}",
        column_name,
        expected,
        actual
    ))]
    TypeMismatch {
        column_name: String,
        expected: String,
        actual: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel from '{addr}'"))]
    CreateChannel {
        addr: String,
        #[snafu(source)]
        error: tonic::transport::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert Arrow type: {}", from))]
    Conversion {
        from: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to decode FlightData"))]
    DecodeFlightData {
        #[snafu(source)]
        error: api::DecodeError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid FlightData, reason: {}", reason))]
    InvalidFlightData {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Not supported: {}", feat))]
    NotSupported { feat: String },

    #[snafu(display("Failed to serde Json"))]
    SerdeJson {
        #[snafu(source)]
        error: serde_json::error::Error,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed arrow operation"))]
    Arrow {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: ArrowError,
    },
}

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::InvalidTlsConfig { .. }
            | Error::InvalidConfigFilePath { .. }
            | Error::FileWatch { .. }
            | Error::TypeMismatch { .. }
            | Error::InvalidFlightData { .. }
            | Error::NotSupported { .. } => StatusCode::InvalidArguments,

            Error::CreateChannel { .. }
            | Error::Conversion { .. }
            | Error::DecodeFlightData { .. }
            | Error::SerdeJson { .. } => StatusCode::Internal,

            Error::Arrow { .. } => StatusCode::Internal,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
