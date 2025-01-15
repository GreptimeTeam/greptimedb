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

pub mod kafka;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_wal::config::MetasrvWalConfig;
use common_wal::options::{KafkaWalOptions, WalOptions, WAL_OPTIONS_KEY};
use snafu::ResultExt;
use store_api::storage::{RegionId, RegionNumber};

use crate::error::{EncodeWalOptionsSnafu, Result};
use crate::kv_backend::KvBackendRef;
use crate::leadership_notifier::LeadershipChangeListener;
use crate::wal_options_allocator::kafka::topic_manager::TopicManager as KafkaTopicManager;

/// Allocates wal options in region granularity.
#[derive(Default)]
pub enum WalOptionsAllocator {
    #[default]
    RaftEngine,
    Kafka(KafkaTopicManager),
}

/// Arc wrapper of WalOptionsAllocator.
pub type WalOptionsAllocatorRef = Arc<WalOptionsAllocator>;

impl WalOptionsAllocator {
    /// Creates a WalOptionsAllocator.
    pub fn new(config: MetasrvWalConfig, kv_backend: KvBackendRef) -> Self {
        match config {
            MetasrvWalConfig::RaftEngine => Self::RaftEngine,
            MetasrvWalConfig::Kafka(kafka_config) => {
                Self::Kafka(KafkaTopicManager::new(kafka_config, kv_backend))
            }
        }
    }

    /// Tries to start the allocator.
    pub async fn start(&self) -> Result<()> {
        match self {
            Self::RaftEngine => Ok(()),
            Self::Kafka(kafka_topic_manager) => kafka_topic_manager.start().await,
        }
    }

    /// Allocates a wal options for a region.
    pub fn alloc(&self) -> Result<WalOptions> {
        match self {
            Self::RaftEngine => Ok(WalOptions::RaftEngine),
            Self::Kafka(topic_manager) => {
                let topic = topic_manager.select()?;
                Ok(WalOptions::Kafka(KafkaWalOptions {
                    topic: topic.clone(),
                }))
            }
        }
    }

    /// Allocates a batch of wal options where each wal options goes to a region.
    pub fn alloc_batch(&self, num_regions: usize) -> Result<Vec<WalOptions>> {
        match self {
            WalOptionsAllocator::RaftEngine => Ok(vec![WalOptions::RaftEngine; num_regions]),
            WalOptionsAllocator::Kafka(topic_manager) => {
                let options_batch = topic_manager
                    .select_batch(num_regions)?
                    .into_iter()
                    .map(|topic| {
                        WalOptions::Kafka(KafkaWalOptions {
                            topic: topic.clone(),
                        })
                    })
                    .collect();
                Ok(options_batch)
            }
        }
    }

    /// Returns true if it's the remote WAL.
    pub fn is_remote_wal(&self) -> bool {
        matches!(&self, WalOptionsAllocator::Kafka(_))
    }
}

#[async_trait]
impl LeadershipChangeListener for WalOptionsAllocator {
    fn name(&self) -> &str {
        "WalOptionsAllocator"
    }

    async fn on_leader_start(&self) -> Result<()> {
        self.start().await
    }

    async fn on_leader_stop(&self) -> Result<()> {
        Ok(())
    }
}

/// Allocates a wal options for each region. The allocated wal options is encoded immediately.
pub fn allocate_region_wal_options(
    regions: Vec<RegionNumber>,
    wal_options_allocator: &WalOptionsAllocator,
) -> Result<HashMap<RegionNumber, String>> {
    let wal_options = wal_options_allocator
        .alloc_batch(regions.len())?
        .into_iter()
        .map(|wal_options| {
            serde_json::to_string(&wal_options).context(EncodeWalOptionsSnafu { wal_options })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(regions.into_iter().zip(wal_options).collect())
}

/// Inserts wal options into options.
pub fn prepare_wal_options(
    options: &mut HashMap<String, String>,
    region_id: RegionId,
    region_wal_options: &HashMap<RegionNumber, String>,
) {
    if let Some(wal_options) = region_wal_options.get(&region_id.region_number()) {
        options.insert(WAL_OPTIONS_KEY.to_string(), wal_options.clone());
    }
}

#[cfg(test)]
mod tests {
    use common_wal::config::kafka::common::{KafkaConnectionConfig, KafkaTopicConfig};
    use common_wal::config::kafka::MetasrvKafkaConfig;
    use common_wal::test_util::run_test_with_kafka_wal;

    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::wal_options_allocator::kafka::topic_selector::RoundRobinTopicSelector;

    // Tests that the wal options allocator could successfully allocate raft-engine wal options.
    #[tokio::test]
    async fn test_allocator_with_raft_engine() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let wal_config = MetasrvWalConfig::RaftEngine;
        let allocator = WalOptionsAllocator::new(wal_config, kv_backend);
        allocator.start().await.unwrap();

        let num_regions = 32;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = allocate_region_wal_options(regions.clone(), &allocator).unwrap();

        let encoded_wal_options = serde_json::to_string(&WalOptions::RaftEngine).unwrap();
        let expected = regions
            .into_iter()
            .zip(vec![encoded_wal_options; num_regions as usize])
            .collect();
        assert_eq!(got, expected);
    }

    // Tests that the wal options allocator could successfully allocate Kafka wal options.
    #[tokio::test]
    async fn test_allocator_with_kafka() {
        run_test_with_kafka_wal(|broker_endpoints| {
            Box::pin(async {
                let topics = (0..256)
                    .map(|i| format!("test_allocator_with_kafka_{}_{}", i, uuid::Uuid::new_v4()))
                    .collect::<Vec<_>>();

                // Creates a topic manager.
                let kafka_topic = KafkaTopicConfig {
                    replication_factor: broker_endpoints.len() as i16,
                    ..Default::default()
                };
                let config = MetasrvKafkaConfig {
                    connection: KafkaConnectionConfig {
                        broker_endpoints,
                        ..Default::default()
                    },
                    kafka_topic,
                    ..Default::default()
                };
                let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
                let mut topic_manager = KafkaTopicManager::new(config.clone(), kv_backend);
                // Replaces the default topic pool with the constructed topics.
                topic_manager.topic_pool.topics.clone_from(&topics);
                // Replaces the default selector with a round-robin selector without shuffled.
                topic_manager.topic_selector = Arc::new(RoundRobinTopicSelector::default());

                // Creates an options allocator.
                let allocator = WalOptionsAllocator::Kafka(topic_manager);
                allocator.start().await.unwrap();

                let num_regions = 32;
                let regions = (0..num_regions).collect::<Vec<_>>();
                let got = allocate_region_wal_options(regions.clone(), &allocator).unwrap();

                // Check the allocated wal options contain the expected topics.
                let expected = (0..num_regions)
                    .map(|i| {
                        let options = WalOptions::Kafka(KafkaWalOptions {
                            topic: topics[i as usize].clone(),
                        });
                        (i, serde_json::to_string(&options).unwrap())
                    })
                    .collect::<HashMap<_, _>>();
                assert_eq!(got, expected);
            })
        })
        .await;
    }
}
