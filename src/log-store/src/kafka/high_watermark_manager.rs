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

use std::sync::Arc;
use std::time::Duration;

use common_telemetry::error;
use dashmap::DashMap;
use store_api::logstore::provider::KafkaProvider;

use crate::error::Result;
use crate::kafka::client_manager::ClientManagerRef;

/// Running background task to update high watermark for each topic.
pub(crate) struct HighWatermarkManager {
    /// Interval to update high watermark.
    update_interval: Duration,
    /// The high watermark for each topic.
    high_watermark: Arc<DashMap<Arc<KafkaProvider>, u64>>,
    /// Client manager to send requests.
    client_manager: ClientManagerRef,
}

impl HighWatermarkManager {
    pub(crate) fn new(
        update_interval: Duration,
        high_watermark: Arc<DashMap<Arc<KafkaProvider>, u64>>,
        client_manager: ClientManagerRef,
    ) -> Self {
        Self {
            update_interval,
            high_watermark,
            client_manager,
        }
    }

    pub(crate) async fn run(self) {
        common_runtime::spawn_global(async move {
            loop {
                tokio::time::sleep(self.update_interval).await;
                if let Err(e) = self.try_update().await {
                    error!(e; "Failed to update high watermark");
                }
            }
        });
    }

    pub(crate) async fn try_update(&self) -> Result<()> {
        for iterator_element in self.high_watermark.iter() {
            let producer = self
                .client_manager
                .get_or_insert(iterator_element.key())
                .await?
                .producer()
                .clone();
            producer.update_high_watermark().await?;
        }
        Ok(())
    }
}
