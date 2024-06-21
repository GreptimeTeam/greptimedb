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

use std::time::Duration;

use futures::future::BoxFuture;

pub async fn wait_condition_fn<F, T, U>(
    timeout: Duration,
    check: F,
    condition: U,
    retry_interval: Duration,
) where
    F: Fn() -> BoxFuture<'static, T>,
    U: Fn(T) -> bool,
{
    tokio::time::timeout(timeout, async move {
        loop {
            if condition(check().await) {
                break;
            }
            tokio::time::sleep(retry_interval).await
        }
    })
    .await
    .unwrap();
}
