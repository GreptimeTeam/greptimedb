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

use crate::utils::info;

/// Check health of the processing.
#[async_trait::async_trait]
pub trait HealthChecker: Send + Sync {
    async fn check(&self);

    fn wait_timeout(&self) -> Duration;
}

/// Http health checker.
pub struct HttpHealthChecker {
    pub url: String,
}

#[async_trait::async_trait]
impl HealthChecker for HttpHealthChecker {
    async fn check(&self) {
        loop {
            match reqwest::get(&self.url).await {
                Ok(resp) => {
                    if resp.status() == 200 {
                        info!("Health checked!");
                        return;
                    }
                    info!("Failed to check health, status: {}", resp.status());
                }
                Err(err) => {
                    info!("Failed to check health, error: {err:?}");
                }
            }

            info!("Checking health later...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    fn wait_timeout(&self) -> Duration {
        Duration::from_secs(5)
    }
}
