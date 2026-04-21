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

#![allow(dead_code)]

use std::future::Future;
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetryConfig {
    pub(crate) max_retries: usize,
    pub(crate) initial_backoff: Duration,
    pub(crate) max_backoff: Duration,
    pub(crate) multiplier: u32,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(300),
            multiplier: 2,
        }
    }
}

impl RetryConfig {
    pub(crate) fn backoff_delay(&self, attempt: usize) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        let mut delay = self.initial_backoff;
        for _ in 1..attempt {
            delay = delay.saturating_mul(self.multiplier);
            if delay >= self.max_backoff {
                return self.max_backoff;
            }
        }
        delay.min(self.max_backoff)
    }
}

pub(crate) async fn retry_async<T, E, Op, Fut, Pred>(
    config: &RetryConfig,
    mut op: Op,
    is_retryable: Pred,
) -> std::result::Result<T, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, E>>,
    Pred: Fn(&E) -> bool,
{
    let mut attempt = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) if attempt < config.max_retries && is_retryable(&error) => {
                let delay = config.backoff_delay(attempt + 1);
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_backoff_delay_clamps_to_max() {
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_secs(2),
            max_backoff: Duration::from_secs(5),
            multiplier: 3,
        };

        assert_eq!(config.backoff_delay(0), Duration::ZERO);
        assert_eq!(config.backoff_delay(1), Duration::from_secs(2));
        assert_eq!(config.backoff_delay(2), Duration::from_secs(5));
        assert_eq!(config.backoff_delay(3), Duration::from_secs(5));
    }

    #[tokio::test]
    async fn test_retry_async_retries_retryable_error_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let config = RetryConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..Default::default()
        };

        let result = retry_async(
            &config,
            {
                let attempts = attempts.clone();
                move || {
                    let attempts = attempts.clone();
                    async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst);
                        if current < 2 {
                            Err("retryable")
                        } else {
                            Ok("done")
                        }
                    }
                }
            },
            |error| *error == "retryable",
        )
        .await;

        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_async_stops_on_non_retryable_error() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let config = RetryConfig {
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..Default::default()
        };

        let result: std::result::Result<(), &str> = retry_async(
            &config,
            {
                let attempts = attempts.clone();
                move || {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err("fatal")
                    }
                }
            },
            |error| *error == "retryable",
        )
        .await;

        assert_eq!(result, Err("fatal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_async_returns_last_error_after_reaching_limit() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            ..Default::default()
        };

        let result: std::result::Result<(), usize> = retry_async(
            &config,
            {
                let attempts = attempts.clone();
                move || {
                    let attempts = attempts.clone();
                    async move {
                        let current = attempts.fetch_add(1, Ordering::SeqCst);
                        Err(current)
                    }
                }
            },
            |_| true,
        )
        .await;

        assert_eq!(result, Err(2));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
