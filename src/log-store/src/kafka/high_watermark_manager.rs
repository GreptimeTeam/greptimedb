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
use tokio::time::{interval, MissedTickBehavior};

use crate::error::Result;
use crate::kafka::client_manager::ClientManagerRef;

/// HighWatermarkManager is responsible for periodically updating the high watermark
/// (latest existing record offset) for each Kafka topic.
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

    /// Starts the high watermark manager as a background task
    ///
    /// This spawns a task that periodically queries Kafka for the latest
    /// high watermark values for all registered topics and updates the shared map.
    pub(crate) async fn run(self) {
        common_runtime::spawn_global(async move {
            let mut interval = interval(self.update_interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = self.try_update().await {
                    error!(e; "Failed to update high watermark");
                }
            }
        });
    }

    /// Attempts to update the high watermark for all registered topics
    ///
    /// Iterates through all topics in the high watermark map, obtains a producer
    /// for each topic, and requests an update of the high watermark value.
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

#[cfg(test)]
mod tests {
    use common_wal::test_util::run_test_with_kafka_wal;
    use store_api::storage::RegionId;

    use super::*;
    use crate::kafka::test_util::{prepare, record};

    #[tokio::test]
    async fn test_try_update_high_watermark() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let (manager, topics) =
                    prepare("test_try_update_high_watermark", 1, broker_endpoints).await;
                let manager = Arc::new(manager);
                let high_watermark_manager = HighWatermarkManager::new(
                    Duration::from_millis(100),
                    manager.high_watermark().clone(),
                    manager.clone(),
                );
                let high_watermark = high_watermark_manager.high_watermark.clone();
                high_watermark_manager.run().await;

                let topic = topics[0].clone();
                let provider = Arc::new(KafkaProvider::new(topic.to_string()));
                let producer = manager
                    .get_or_insert(&provider)
                    .await
                    .unwrap()
                    .producer()
                    .clone();

                tokio::time::sleep(Duration::from_millis(150)).await;
                let current_high_watermark = *high_watermark.get(&provider).unwrap();
                assert_eq!(current_high_watermark, 0);

                let record = vec![record()];
                let region = RegionId::new(1, 1);
                producer.produce(region, record.clone()).await.unwrap();
                tokio::time::sleep(Duration::from_millis(150)).await;
                let current_high_watermark = *high_watermark.get(&provider).unwrap();
                assert_eq!(current_high_watermark, record.len() as u64);
            })
        })
        .await
    }
}
