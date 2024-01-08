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
use common_error::{GREPTIME_DB_HEADER_ERROR_CODE, GREPTIME_DB_HEADER_ERROR_MSG};
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tonic::{Code, Status};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal Flight messages, reason: {}", reason))]
    IllegalFlightMessages { reason: String, location: Location },

    #[snafu(display("Failed to do Flight get, code: {}", tonic_code))]
    FlightGet {
        addr: String,
        tonic_code: Code,
        source: BoxedError,
    },

    #[snafu(display("Failure occurs during handling request"))]
    HandleRequest {
        location: Location,
        source: BoxedError,
    },

    #[snafu(display("Failed to convert FlightData"))]
    ConvertFlightData {
        location: Location,
        source: common_grpc::Error,
    },

    #[snafu(display("Column datatype error"))]
    ColumnDataType {
        location: Location,
        source: api::error::Error,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState { err_msg: String, location: Location },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, location: Location },

    #[snafu(display("Failed to create gRPC channel, peer address: {}", addr))]
    CreateChannel {
        addr: String,
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to request RegionServer, code: {}", code))]
    RegionServer { code: Code, source: BoxedError },

    // Server error carried in Tonic Status's metadata.
    #[snafu(display("{}", msg))]
    Server { code: StatusCode, msg: String },

    #[snafu(display("Illegal Database response: {err_msg}"))]
    IllegalDatabaseResponse { err_msg: String },

    #[snafu(display("Failed to send request with streaming: {}", err_msg))]
    ClientStreaming { err_msg: String, location: Location },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalFlightMessages { .. }
            | Error::ColumnDataType { .. }
            | Error::MissingField { .. }
            | Error::IllegalDatabaseResponse { .. }
            | Error::ClientStreaming { .. } => StatusCode::Internal,

            Error::Server { code, .. } => *code,
            Error::FlightGet { source, .. }
            | Error::HandleRequest { source, .. }
            | Error::RegionServer { source, .. } => source.status_code(),
            Error::CreateChannel { source, .. } | Error::ConvertFlightData { source, .. } => {
                source.status_code()
            }
            Error::IllegalGrpcClientState { .. } => StatusCode::Unexpected,
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl From<Status> for Error {
    fn from(e: Status) -> Self {
        fn get_metadata_value(e: &Status, key: &str) -> Option<String> {
            e.metadata()
                .get(key)
                .and_then(|v| String::from_utf8(v.as_bytes().to_vec()).ok())
        }

        let code = get_metadata_value(&e, GREPTIME_DB_HEADER_ERROR_CODE)
            .and_then(|s| {
                if let Ok(code) = s.parse::<u32>() {
                    StatusCode::from_u32(code)
                } else {
                    None
                }
            })
            .unwrap_or(StatusCode::Unknown);

        let msg = get_metadata_value(&e, GREPTIME_DB_HEADER_ERROR_MSG)
            .unwrap_or_else(|| e.message().to_string());

        Self::Server { code, msg }
    }
}

impl Error {
    pub fn should_retry(&self) -> bool {
        !matches!(
            self,
            Self::RegionServer {
                code: Code::InvalidArgument,
                ..
            }
        )
    }
}
