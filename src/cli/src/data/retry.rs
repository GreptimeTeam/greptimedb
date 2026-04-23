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

use std::time::Duration;

use backon::ExponentialBuilder;

pub(crate) fn default_retry_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_secs(1))
        .with_max_delay(Duration::from_secs(300))
        .with_factor(2.0)
        // This is the number of retries after the initial attempt.
        .with_max_times(3)
        .with_jitter()
}

#[cfg(test)]
mod tests {
    use std::future::ready;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use backon::Retryable;

    use super::*;

    #[tokio::test]
    async fn test_retry_policy_retries_retryable_error_until_success() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = ({
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
        })
        .retry(default_retry_policy())
        .when(|error| *error == "retryable")
        .sleep(|_| ready(()))
        .await;

        assert_eq!(result, Ok("done"));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_policy_stops_on_non_retryable_error() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result: std::result::Result<(), &str> = ({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err("fatal")
                }
            }
        })
        .retry(default_retry_policy())
        .when(|error| *error == "retryable")
        .sleep(|_| ready(()))
        .await;

        assert_eq!(result, Err("fatal"));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_retry_policy_returns_last_error_after_reaching_limit() {
        let attempts = Arc::new(AtomicUsize::new(0));

        let result: std::result::Result<(), usize> = ({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let current = attempts.fetch_add(1, Ordering::SeqCst);
                    Err(current)
                }
            }
        })
        .retry(default_retry_policy().with_max_times(2))
        .when(|_| true)
        .sleep(|_| ready(()))
        .await;

        assert_eq!(result, Err(2));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }
}
