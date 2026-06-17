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

use common_error::ext::RetryHint;

/// Maps an rskafka client error to a conservative retry hint.
///
/// rskafka already retries most transient Kafka/network errors internally. This helper only
/// marks errors retryable when rskafka has exhausted its own retry loop or returns an explicit
/// client timeout.
pub fn rskafka_client_error_to_retry_hint(error: &rskafka::client::error::Error) -> RetryHint {
    use rskafka::client::error::Error;

    match error {
        Error::RetryFailed(_)
        | Error::Timeout
        | Error::Connection(rskafka::ConnectionError::RetryFailed(_)) => RetryHint::Retryable,
        _ => RetryHint::NonRetryable,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use common_error::ext::RetryHint;
    use rskafka::BackoffError;
    use rskafka::client::error::{Error as KafkaClientError, ProtocolError, RequestContext};

    use super::*;

    fn retry_failed_error() -> KafkaClientError {
        KafkaClientError::RetryFailed(BackoffError::DeadlineExceded {
            deadline: Duration::from_secs(1),
            source: Box::new(std::io::Error::other("retry failed")),
        })
    }

    #[test]
    fn test_retry_failed_hint_is_retryable() {
        assert_eq!(
            rskafka_client_error_to_retry_hint(&retry_failed_error()),
            RetryHint::Retryable
        );
    }

    #[test]
    fn test_timeout_hint_is_retryable() {
        assert_eq!(
            rskafka_client_error_to_retry_hint(&KafkaClientError::Timeout),
            RetryHint::Retryable
        );
    }

    #[test]
    fn test_connection_retry_failed_hint_is_retryable() {
        let err = KafkaClientError::Connection(rskafka::ConnectionError::RetryFailed(
            BackoffError::DeadlineExceded {
                deadline: Duration::from_secs(1),
                source: Box::new(std::io::Error::other("retry failed")),
            },
        ));

        assert_eq!(
            rskafka_client_error_to_retry_hint(&err),
            RetryHint::Retryable
        );
    }

    #[test]
    fn test_raw_protocol_error_hint_is_non_retryable() {
        let err = KafkaClientError::ServerError {
            protocol_error: ProtocolError::NetworkException,
            error_message: None,
            request: RequestContext::Topic("test_topic".to_string()),
            response: None,
            is_virtual: false,
        };

        assert_eq!(
            rskafka_client_error_to_retry_hint(&err),
            RetryHint::NonRetryable
        );
    }
}
