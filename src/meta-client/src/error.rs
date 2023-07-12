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

use common_error::ext::ErrorExt;
use common_error::status_code::StatusCode;
use snafu::{Location, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState { err_msg: String, location: Location },

    #[snafu(display("Tonic internal error, source: {}", source))]
    TonicStatus {
        source: tonic::Status,
        location: Location,
    },

    #[snafu(display("Failed to ask leader from all endpoints"))]
    AskLeader { location: Location },

    #[snafu(display("No leader, should ask leader first"))]
    NoLeader { location: Location },

    #[snafu(display("Failed to create gRPC channel, source: {}", source))]
    CreateChannel {
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("{} not started", name))]
    NotStarted { name: String, location: Location },

    #[snafu(display("Failed to send heartbeat: {}", err_msg))]
    SendHeartbeat { err_msg: String, location: Location },

    #[snafu(display("Failed create heartbeat stream to server"))]
    CreateHeartbeatStream { location: Location },

    #[snafu(display("Invalid response header, source: {}", source))]
    InvalidResponseHeader {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv request, source: {}", source))]
    ConvertMetaRequest {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv response, source: {}", source))]
    ConvertMetaResponse {
        location: Location,
        source: common_meta::error::Error,
    },
}

#[allow(dead_code)]
pub type Result<T> = std::result::Result<T, Error>;

impl ErrorExt for Error {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn status_code(&self) -> StatusCode {
        match self {
            Error::IllegalGrpcClientState { .. }
            | Error::TonicStatus { .. }
            | Error::AskLeader { .. }
            | Error::NoLeader { .. }
            | Error::NotStarted { .. }
            | Error::SendHeartbeat { .. }
            | Error::CreateHeartbeatStream { .. }
            | Error::CreateChannel { .. } => StatusCode::Internal,

            Error::InvalidResponseHeader { source, .. }
            | Error::ConvertMetaRequest { source, .. }
            | Error::ConvertMetaResponse { source, .. } => source.status_code(),
        }
    }
}
