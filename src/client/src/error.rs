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

use common_error::define_from_tonic_status;
use common_error::ext::{BoxedError, ErrorExt};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tonic::Code;
use tonic::metadata::errors::InvalidMetadataValue;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal Flight messages, reason: {}", reason))]
    IllegalFlightMessages {
        reason: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to do Flight get, code: {}", tonic_code))]
    FlightGet {
        addr: String,
        tonic_code: Code,
        source: BoxedError,
    },

    #[snafu(display("Failed to convert FlightData"))]
    ConvertFlightData {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::Error,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField {
        field: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to create gRPC channel, peer address: {}", addr))]
    CreateChannel {
        addr: String,
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to create Tls channel manager"))]
    CreateTlsChannel {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("Failed to request RegionServer {}, code: {}", addr, code))]
    RegionServer {
        addr: String,
        code: Code,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to request FlowServer {}, code: {}", addr, code))]
    FlowServer {
        addr: String,
        code: Code,
        source: BoxedError,
        #[snafu(implicit)]
        location: Location,
    },

    // Server error carried in Tonic Status's metadata.
    #[snafu(display("{}", msg))]
    Server {
        code: StatusCode,
        msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Illegal Database response: {err_msg}"))]
    IllegalDatabaseResponse {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid Tonic metadata value"))]
    InvalidTonicMetadataValue {
        #[snafu(source)]
        error: InvalidMetadataValue,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert Schema"))]
    ConvertSchema {
        #[snafu(implicit)]
        location: Location,
        source: datatypes::error::Error,
    },

    #[snafu(display("{}", msg))]
    Tonic {
        code: StatusCode,
        msg: String,
        tonic_code: Code,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("External error"))]
    External {
        #[snafu(implicit)]
        location: Location,
        source: BoxedError,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalFlightMessages { .. }
            | Error::MissingField { .. }
            | Error::IllegalDatabaseResponse { .. } => StatusCode::Internal,

            Error::Server { code, .. } | Error::Tonic { code, .. } => *code,
            Error::FlightGet { source, .. }
            | Error::RegionServer { source, .. }
            | Error::FlowServer { source, .. } => source.status_code(),
            Error::CreateChannel { source, .. }
            | Error::ConvertFlightData { source, .. }
            | Error::CreateTlsChannel { source, .. } => source.status_code(),
            Error::IllegalGrpcClientState { .. } => StatusCode::Unexpected,
            Error::InvalidTonicMetadataValue { .. } => StatusCode::InvalidArguments,
            Error::ConvertSchema { source, .. } => source.status_code(),
            Error::External { source, .. } => source.status_code(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

define_from_tonic_status!(Error, Tonic);

impl Error {
    /// Returns the gRPC status code if this error is caused by a gRPC request failure.
    pub fn tonic_code(&self) -> Option<Code> {
        match self {
            Self::FlightGet { tonic_code, .. }
            | Self::RegionServer {
                code: tonic_code, ..
            }
            | Self::FlowServer {
                code: tonic_code, ..
            }
            | Self::Tonic { tonic_code, .. } => Some(*tonic_code),
            _ => None,
        }
    }

    /// Returns true if the error is a connection error that may be resolved by retrying the request.
    pub fn is_connection_error(&self) -> bool {
        matches!(self.tonic_code(), Some(Code::Unavailable))
    }

    pub fn should_retry(&self) -> bool {
        self.is_connection_error()
            || matches!(
                self.tonic_code(),
                Some(Code::Cancelled) | Some(Code::DeadlineExceeded)
            )
    }
}
