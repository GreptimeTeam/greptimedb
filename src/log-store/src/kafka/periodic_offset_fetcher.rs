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

use common_telemetry::{debug, error, info};
use tokio::time::{interval, MissedTickBehavior};

use crate::error::Result;
use crate::kafka::client_manager::ClientManagerRef;

/// PeriodicOffsetFetcher is responsible for periodically updating the offset
/// for each Kafka topic.
pub(crate) struct PeriodicOffsetFetcher {
    /// Interval to fetch the latest offset.
    interval: Duration,
    /// Client manager to send requests.
    client_manager: ClientManagerRef,
}

impl PeriodicOffsetFetcher {
    pub(crate) fn new(interval: Duration, client_manager: ClientManagerRef) -> Self {
        Self {
            interval,
            client_manager,
        }
    }

    /// Starts the offset fetcher as a background task
    ///
    /// This spawns a task that periodically queries Kafka for the latest
    /// offset values for all registered topics and updates the shared map.
    pub(crate) async fn run(self) {
        common_runtime::spawn_global(async move {
            info!("PeriodicOffsetFetcher started");
            let mut interval = interval(self.interval);
            interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(e) = self.try_update().await {
                    error!(e; "Failed to update latest offset");
                }
            }
        });
    }

    /// Tries to refresh the latest offset for every registered topic.
    ///
    /// For each topic in the stats map, retrieves its producer and requests
    /// an update of the latest offset from Kafka.
    pub(crate) async fn try_update(&self) -> Result<()> {
        let topics = self.client_manager.list_topics().await;
        for topic in topics.iter() {
            debug!("Fetching latest offset for topic: {}", topic.topic);
            let producer = self
                .client_manager
                .get_or_insert(topic)
                .await?
                .producer()
                .clone();
            producer.fetch_latest_offset().await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;
    use store_api::logstore::provider::KafkaProvider;
    use store_api::storage::RegionId;

    use super::*;
    use crate::kafka::test_util::{prepare, record};

    #[tokio::test]
    async fn test_try_update_latest_offset() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let broker_endpoints = get_kafka_endpoints();

        let (manager, topics) = prepare("test_try_update_latest_offset", 1, broker_endpoints).await;
        let manager = Arc::new(manager);
        let fetcher = PeriodicOffsetFetcher::new(Duration::from_millis(100), manager.clone());
        let topic_stats = manager.topic_stats().clone();
        fetcher.run().await;

        let topic = topics[0].clone();
        let provider = Arc::new(KafkaProvider::new(topic.to_string()));
        let producer = manager
            .get_or_insert(&provider)
            .await
            .unwrap()
            .producer()
            .clone();

        tokio::time::sleep(Duration::from_millis(150)).await;
        let current_latest_offset = topic_stats.get(&provider).unwrap().latest_offset;
        assert_eq!(current_latest_offset, 0);

        let record = vec![record()];
        let region = RegionId::new(1, 1);
        producer.produce(region, record.clone()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let current_latest_offset = topic_stats.get(&provider).unwrap().latest_offset;
        assert_eq!(current_latest_offset, record.len() as u64);
    }
}
