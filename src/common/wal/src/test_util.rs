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

use common_telemetry::warn;
use futures_util::future::BoxFuture;

pub async fn run_test_with_kafka_wal<F>(test: F)
where
    F: FnOnce(Vec<String>) -> BoxFuture<'static, ()>,
{
    let Ok(endpoints) = std::env::var("GT_KAFKA_ENDPOINTS") else {
        warn!("The endpoints is empty, skipping the test");
        return;
    };

    let endpoints = endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>();

    test(endpoints).await
}

/// Get the kafka endpoints from the environment variable `GT_KAFKA_ENDPOINTS`.
///
/// The format of the environment variable is:
/// ```
/// GT_KAFKA_ENDPOINTS=localhost:9092,localhost:9093
/// ```
pub fn get_kafka_endpoints() -> Vec<String> {
    let endpoints = std::env::var("GT_KAFKA_ENDPOINTS").unwrap();
    endpoints
        .split(',')
        .map(|s| s.trim().to_string())
        .collect::<Vec<_>>()
}

#[macro_export]
/// Skip the test if the environment variable `GT_KAFKA_ENDPOINTS` is not set.
///
/// The format of the environment variable is:
/// ```
/// GT_KAFKA_ENDPOINTS=localhost:9092,localhost:9093
/// ```
macro_rules! maybe_skip_kafka_integration_test {
    () => {
        if std::env::var("GT_KAFKA_ENDPOINTS").is_err() {
            common_telemetry::warn!("The endpoints is empty, skipping the test");
            return;
        }
    };
}
