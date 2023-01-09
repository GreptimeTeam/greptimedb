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

use common_error::prelude::*;

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Illegal Flight messages, reason: {}", reason))]
    IllegalFlightMessages {
        reason: String,
        backtrace: Backtrace,
    },

    #[snafu(display(
        "Failed to do Flight get, addr: {}, code: {}, err_msg: {}",
        addr,
        code,
        err_msg
    ))]
    FlightGet {
        addr: String,
        code: u32,
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Failed to convert FlightData, source: {}", source))]
    ConvertFlightData {
        #[snafu(backtrace)]
        source: common_grpc::Error,
    },

    #[snafu(display("Column datatype error, source: {}", source))]
    ColumnDataType {
        #[snafu(backtrace)]
        source: api::error::Error,
    },

    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        backtrace: Backtrace,
    },

    #[snafu(display("Missing required field in protobuf, field: {}", field))]
    MissingField { field: String, backtrace: Backtrace },

    #[snafu(display(
        "Failed to create gRPC channel, peer address: {}, source: {}",
        addr,
        source
    ))]
    CreateChannel {
        addr: String,
        #[snafu(backtrace)]
        source: common_grpc::error::Error,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalFlightMessages { .. }
            | Error::FlightGet { .. }
            | Error::ColumnDataType { .. }
            | Error::MissingField { .. } => StatusCode::Internal,
            Error::CreateChannel { source, .. } | Error::ConvertFlightData { source } => {
                source.status_code()
            }
            Error::IllegalGrpcClientState { .. } => StatusCode::Unexpected,
        }
    }

    fn backtrace_opt(&self) -> Option<&Backtrace> {
        ErrorCompat::backtrace(self)
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}
