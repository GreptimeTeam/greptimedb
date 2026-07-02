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

use common_error::ext::{RetryHint, retry_hint_from_io_error};

/// Converts an etcd client error into a conservative retry hint.
pub fn retry_hint_from_etcd_error(error: &etcd_client::Error) -> RetryHint {
    match error {
        etcd_client::Error::IoError(error) => retry_hint_from_io_error(error),
        etcd_client::Error::TransportError(_)
        | etcd_client::Error::EndpointError(_)
        | etcd_client::Error::WatchError(_)
        | etcd_client::Error::LeaseKeepAliveError(_)
        | etcd_client::Error::ElectError(_) => RetryHint::Retryable,
        etcd_client::Error::GRpcStatus(status) => retry_hint_from_etcd_grpc_code(status.code()),
        etcd_client::Error::InvalidArgs(_)
        | etcd_client::Error::InvalidUri(_)
        | etcd_client::Error::Utf8Error(_)
        | etcd_client::Error::InvalidHeaderValue(_)
        | etcd_client::Error::EndpointsNotManaged => RetryHint::NonRetryable,
    }
}

/// Converts a tonic status code from an external backend into a retry hint.
fn retry_hint_from_etcd_grpc_code(code: tonic::Code) -> RetryHint {
    match code {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded | tonic::Code::Aborted => {
            RetryHint::Retryable
        }
        _ => RetryHint::NonRetryable,
    }
}

#[cfg(test)]
mod tests {
    use common_error::ext::RetryHint;

    use super::*;

    #[test]
    fn test_etcd_grpc_status_retry_hint() {
        assert_eq!(
            retry_hint_from_etcd_grpc_code(tonic::Code::Unavailable),
            RetryHint::Retryable
        );
        assert_eq!(
            retry_hint_from_etcd_grpc_code(tonic::Code::DeadlineExceeded),
            RetryHint::Retryable
        );
        assert_eq!(
            retry_hint_from_etcd_grpc_code(tonic::Code::Aborted),
            RetryHint::Retryable
        );
        assert_eq!(
            retry_hint_from_etcd_grpc_code(tonic::Code::ResourceExhausted),
            RetryHint::NonRetryable
        );
        assert_eq!(
            retry_hint_from_etcd_grpc_code(tonic::Code::InvalidArgument),
            RetryHint::NonRetryable
        );
    }
}
