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

use std::cmp;
use std::future::Future;
use std::time::Duration;

use common_error::ext::ErrorExt;
use common_telemetry::warn;

#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_retries: usize,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
}

impl RetryConfig {
    pub fn new(max_retries: usize, initial_backoff: Duration) -> Self {
        Self {
            max_retries,
            initial_backoff,
            max_backoff: Duration::from_secs(300),
        }
    }

    fn backoff_delay(&self, attempt: usize) -> Duration {
        let factor = 1u32.checked_shl(attempt as u32).unwrap_or(u32::MAX);
        cmp::min(
            self.max_backoff,
            self.initial_backoff.saturating_mul(factor),
        )
    }
}

#[derive(Debug)]
pub struct RetryFailure<E> {
    pub error: E,
    pub retries: usize,
}

pub async fn run_with_retry<T, E, F, Fut>(
    label: &str,
    retry: &RetryConfig,
    mut op: F,
) -> std::result::Result<(T, usize), RetryFailure<E>>
where
    E: ErrorExt,
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
{
    let mut attempt = 0usize;
    loop {
        match op().await {
            Ok(v) => return Ok((v, attempt)),
            Err(err) => {
                if attempt >= retry.max_retries || !err.status_code().is_retryable() {
                    return Err(RetryFailure {
                        error: err,
                        retries: attempt,
                    });
                }
                let delay = retry.backoff_delay(attempt);
                warn!(
                    "{}: failed (retry {}/{}): {}; waiting {:?}",
                    label,
                    attempt + 1,
                    retry.max_retries,
                    err,
                    delay
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use snafu::GenerateImplicitData;

    use super::*;
    use crate::data::import_v2::error::Error;

    #[tokio::test]
    async fn test_retry_succeeds_after_failures() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let retry = RetryConfig::new(3, Duration::from_millis(1));
        let outcome = run_with_retry("retry-test", &retry, {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    if current < 2 {
                        Err(Error::StateOperation {
                            operation: "test".to_string(),
                            path: "/tmp/test".to_string(),
                            error: std::io::Error::other("transient"),
                            location: snafu::Location::generate(),
                        })
                    } else {
                        Ok(42)
                    }
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(outcome.0, 42);
        assert_eq!(outcome.1, 2);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_non_retryable_stops_immediately() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let retry = RetryConfig::new(3, Duration::from_millis(1));
        let err = run_with_retry("retry-test", &retry, {
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err::<(), _>(Error::StateTargetMismatch {
                        expected: "a".to_string(),
                        found: "b".to_string(),
                        location: snafu::Location::generate(),
                    })
                }
            }
        })
        .await
        .unwrap_err();
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(err.error.to_string().contains("State target mismatch"));
    }
}
