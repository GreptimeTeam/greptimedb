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

use std::future::Future;
use std::time::Duration;

use common_telemetry::warn;

pub async fn retry_with_backoff<T, E, Fut, F>(
    mut operation: F,
    max_attempts: usize,
    init_backoff: Duration,
    max_backoff: Duration,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Debug,
{
    let mut backoff = init_backoff;
    for attempt in 0..max_attempts {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) if attempt + 1 == max_attempts => return Err(err),
            Err(err) => {
                let current_attempt = attempt + 1;
                warn!(
                    "Retryable operation failed, attempt: {}, max_attempts: {}, backoff: {:?}, error: {:?}",
                    current_attempt, max_attempts, backoff, err
                );
                tokio::time::sleep(backoff).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        }
    }

    panic!("retry loop should always return")
}
