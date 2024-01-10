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
use common_error::{GREPTIME_DB_HEADER_ERROR_CODE, GREPTIME_DB_HEADER_ERROR_MSG};
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};
use tonic::Status;

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState { err_msg: String, location: Location },

    #[snafu(display("{}", msg))]
    MetaServer { code: StatusCode, msg: String },

    #[snafu(display("Failed to ask leader from all endpoints"))]
    AskLeader { location: Location },

    #[snafu(display("No leader, should ask leader first"))]
    NoLeader { location: Location },

    #[snafu(display("Ask leader timeout"))]
    AskLeaderTimeout {
        location: Location,
        #[snafu(source)]
        error: tokio::time::error::Elapsed,
    },

    #[snafu(display("Failed to create gRPC channel"))]
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

    #[snafu(display("Invalid response header"))]
    InvalidResponseHeader {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv request"))]
    ConvertMetaRequest {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv response"))]
    ConvertMetaResponse {
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Retry exceeded max times({}), message: {}", times, msg))]
    RetryTimesExceeded { times: usize, msg: String },
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
            | Error::AskLeader { .. }
            | Error::NoLeader { .. }
            | Error::AskLeaderTimeout { .. }
            | Error::NotStarted { .. }
            | Error::SendHeartbeat { .. }
            | Error::CreateHeartbeatStream { .. }
            | Error::CreateChannel { .. }
            | Error::RetryTimesExceeded { .. } => StatusCode::Internal,

            Error::MetaServer { code, .. } => *code,

            Error::InvalidResponseHeader { source, .. }
            | Error::ConvertMetaRequest { source, .. }
            | Error::ConvertMetaResponse { source, .. } => source.status_code(),
        }
    }
}

// FIXME(dennis): partial duplicated with src/client/src/error.rs
impl From<Status> for Error {
    fn from(e: Status) -> Self {
        fn get_metadata_value(s: &Status, key: &str) -> Option<String> {
            s.metadata()
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
            .unwrap_or(StatusCode::Internal);

        let msg = get_metadata_value(&e, GREPTIME_DB_HEADER_ERROR_MSG)
            .unwrap_or_else(|| e.message().to_string());

        Self::MetaServer { code, msg }
    }
}
