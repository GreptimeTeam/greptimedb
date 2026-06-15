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

use common_error::define_from_tonic_status;
use common_error::ext::{ErrorExt, RetryHint};
use common_error::status_code::StatusCode;
use common_macro::stack_trace_debug;
use snafu::{Location, Snafu};

#[derive(Snafu)]
#[snafu(visibility(pub))]
#[stack_trace_debug]
pub enum Error {
    #[snafu(display("Illegal GRPC client state: {}", err_msg))]
    IllegalGrpcClientState {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("{}", msg))]
    MetaServer {
        code: StatusCode,
        msg: String,
        tonic_code: tonic::Code,
        retry_hint: RetryHint,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("No leader, should ask leader first"))]
    NoLeader {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Ask leader timeout"))]
    AskLeaderTimeout {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: tokio::time::error::Elapsed,
    },

    #[snafu(display("Failed to create gRPC channel"))]
    CreateChannel {
        #[snafu(implicit)]
        location: Location,
        source: common_grpc::error::Error,
    },

    #[snafu(display("{} not started", name))]
    NotStarted {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to send heartbeat: {}", err_msg))]
    SendHeartbeat {
        err_msg: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed create heartbeat stream to server"))]
    CreateHeartbeatStream {
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Invalid response header"))]
    InvalidResponseHeader {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv request"))]
    ConvertMetaRequest {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to convert Metasrv response"))]
    ConvertMetaResponse {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Failed to get flow stat"))]
    GetFlowStat {
        #[snafu(implicit)]
        location: Location,
        source: common_meta::error::Error,
    },

    #[snafu(display("Retry exceeded max times({}), message: {}", times, msg))]
    RetryTimesExceeded { times: usize, msg: String },

    #[snafu(display("Trying to write to a read-only kv backend: {}", name))]
    ReadOnlyKvBackend {
        name: String,
        #[snafu(implicit)]
        location: Location,
    },

    #[snafu(display("Failed to convert meta config"))]
    ConvertMetaConfig {
        #[snafu(implicit)]
        location: Location,
        #[snafu(source)]
        error: serde_json::Error,
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
            | Error::NoLeader { .. }
            | Error::AskLeaderTimeout { .. }
            | Error::NotStarted { .. }
            | Error::SendHeartbeat { .. }
            | Error::CreateHeartbeatStream { .. }
            | Error::CreateChannel { .. }
            | Error::RetryTimesExceeded { .. }
            | Error::ConvertMetaConfig { .. } => StatusCode::Internal,

            Error::ReadOnlyKvBackend { .. } => StatusCode::Unsupported,

            Error::MetaServer { code, .. } => *code,

            Error::InvalidResponseHeader { source, .. }
            | Error::ConvertMetaRequest { source, .. }
            | Error::ConvertMetaResponse { source, .. }
            | Error::GetFlowStat { source, .. } => source.status_code(),
        }
    }

    fn retry_hint(&self) -> RetryHint {
        match self {
            Error::MetaServer { retry_hint, .. } => *retry_hint,
            Error::InvalidResponseHeader { source, .. }
            | Error::ConvertMetaRequest { source, .. }
            | Error::ConvertMetaResponse { source, .. }
            | Error::GetFlowStat { source, .. } => source.retry_hint(),
            _ => RetryHint::from_status_code(self.status_code()),
        }
    }
}

impl Error {
    pub fn is_exceeded_size_limit(&self) -> bool {
        matches!(
            self,
            Error::MetaServer {
                tonic_code: tonic::Code::OutOfRange,
                ..
            }
        )
    }
}

define_from_tonic_status!(Error, MetaServer);

#[cfg(test)]
mod tests {
    use common_error::ext::{ErrorExt, RetryHint};
    use common_error::{GREPTIME_DB_HEADER_ERROR_CODE, GREPTIME_DB_HEADER_ERROR_RETRY_HINT};
    use tonic::codegen::http::{HeaderMap, HeaderValue};
    use tonic::metadata::MetadataMap;

    use super::*;

    #[test]
    fn test_from_tonic_status_fallbacks_to_status_code() {
        let status = tonic::Status::new(tonic::Code::Internal, "blabla");

        let err: Error = status.into();

        assert_eq!(err.retry_hint(), RetryHint::Retryable);
    }

    #[test]
    fn test_from_tonic_status_fallback_can_be_non_retryable() {
        let mut headers = HeaderMap::new();
        headers.insert(
            GREPTIME_DB_HEADER_ERROR_CODE,
            HeaderValue::from(StatusCode::InvalidArguments as u32),
        );
        let status = tonic::Status::with_metadata(
            tonic::Code::Internal,
            "blabla",
            MetadataMap::from_headers(headers),
        );

        let err: Error = status.into();

        assert_eq!(err.retry_hint(), RetryHint::NonRetryable);
    }

    #[test]
    fn test_from_tonic_status_with_retry_hint() {
        let mut headers = HeaderMap::new();
        headers.insert(
            GREPTIME_DB_HEADER_ERROR_CODE,
            HeaderValue::from(StatusCode::Internal as u32),
        );
        headers.insert(
            GREPTIME_DB_HEADER_ERROR_RETRY_HINT,
            HeaderValue::from_static(RetryHint::Retryable.as_str()),
        );
        let status = tonic::Status::with_metadata(
            tonic::Code::Internal,
            "blabla",
            MetadataMap::from_headers(headers),
        );

        let err: Error = status.into();

        assert_eq!(err.retry_hint(), RetryHint::Retryable);
    }
}
