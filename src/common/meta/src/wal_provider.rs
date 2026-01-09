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

mod selector;
pub(crate) mod topic_creator;
mod topic_manager;
pub(crate) mod topic_pool;

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use common_wal::config::MetasrvWalConfig;
use common_wal::options::{KafkaWalOptions, WAL_OPTIONS_KEY, WalOptions};
use snafu::{ResultExt, ensure};
use store_api::storage::{RegionId, RegionNumber};

use crate::ddl::allocator::wal_options::WalOptionsAllocator;
use crate::error::{EncodeWalOptionsSnafu, InvalidTopicNamePrefixSnafu, Result};
use crate::key::TOPIC_NAME_PATTERN_REGEX;
use crate::kv_backend::KvBackendRef;
use crate::leadership_notifier::LeadershipChangeListener;
pub use crate::wal_provider::topic_creator::{build_kafka_client, build_kafka_topic_creator};
use crate::wal_provider::topic_pool::KafkaTopicPool;

/// Provides wal options in region granularity.
#[derive(Default, Debug)]
pub enum WalProvider {
    #[default]
    RaftEngine,
    Kafka(KafkaTopicPool),
}

/// Arc wrapper of WalProvider.
pub type WalProviderRef = Arc<WalProvider>;

#[async_trait::async_trait]
impl WalOptionsAllocator for WalProvider {
    async fn allocate(
        &self,
        region_numbers: &[RegionNumber],
        skip_wal: bool,
    ) -> Result<HashMap<RegionNumber, String>> {
        let wal_options = self
            .alloc_batch(region_numbers.len(), skip_wal)?
            .into_iter()
            .map(|wal_options| {
                serde_json::to_string(&wal_options).context(EncodeWalOptionsSnafu { wal_options })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(region_numbers.iter().copied().zip(wal_options).collect())
    }
}

impl WalProvider {
    /// Tries to start the provider.
    pub async fn start(&self) -> Result<()> {
        match self {
            Self::RaftEngine => Ok(()),
            Self::Kafka(kafka_topic_manager) => kafka_topic_manager.activate().await,
        }
    }

    /// Allocates a batch of wal options where each wal options goes to a region.
    /// If skip_wal is true, the wal options will be set to Noop regardless of the provider type.
    pub fn alloc_batch(&self, num_regions: usize, skip_wal: bool) -> Result<Vec<WalOptions>> {
        if skip_wal {
            return Ok(vec![WalOptions::Noop; num_regions]);
        }
        match self {
            WalProvider::RaftEngine => Ok(vec![WalOptions::RaftEngine; num_regions]),
            WalProvider::Kafka(topic_manager) => {
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
        matches!(&self, WalProvider::Kafka(_))
    }
}

#[async_trait]
impl LeadershipChangeListener for WalProvider {
    fn name(&self) -> &str {
        "WalProvider"
    }

    async fn on_leader_start(&self) -> Result<()> {
        self.start().await
    }

    async fn on_leader_stop(&self) -> Result<()> {
        Ok(())
    }
}

/// Builds a wal provider based on the given configuration.
pub async fn build_wal_provider(
    config: &MetasrvWalConfig,
    kv_backend: KvBackendRef,
) -> Result<WalProvider> {
    match config {
        MetasrvWalConfig::RaftEngine => Ok(WalProvider::RaftEngine),
        MetasrvWalConfig::Kafka(kafka_config) => {
            let prefix = &kafka_config.kafka_topic.topic_name_prefix;
            ensure!(
                TOPIC_NAME_PATTERN_REGEX.is_match(prefix),
                InvalidTopicNamePrefixSnafu { prefix }
            );
            let topic_creator =
                build_kafka_topic_creator(&kafka_config.connection, &kafka_config.kafka_topic)
                    .await?;
            let topic_pool = KafkaTopicPool::new(kafka_config, kv_backend, topic_creator);
            Ok(WalProvider::Kafka(topic_pool))
        }
    }
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

/// Extracts the topic from the wal options.
pub fn extract_topic_from_wal_options(
    region_id: RegionId,
    region_options: &HashMap<RegionNumber, String>,
) -> Option<String> {
    region_options
        .get(&region_id.region_number())
        .and_then(|wal_options| {
            serde_json::from_str::<WalOptions>(wal_options)
                .ok()
                .and_then(|wal_options| {
                    if let WalOptions::Kafka(kafka_wal_option) = wal_options {
                        Some(kafka_wal_option.topic)
                    } else {
                        None
                    }
                })
        })
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use common_wal::config::kafka::MetasrvKafkaConfig;
    use common_wal::config::kafka::common::KafkaTopicConfig;
    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::test_util::test_kafka_topic_pool;
    use crate::wal_provider::selector::RoundRobinTopicSelector;

    // Tests that the wal provider could successfully allocate raft-engine wal options.
    #[tokio::test]
    async fn test_provider_with_raft_engine() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let wal_config = MetasrvWalConfig::RaftEngine;
        let provider = build_wal_provider(&wal_config, kv_backend).await.unwrap();
        provider.start().await.unwrap();

        let num_regions = 32;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = provider.allocate(&regions, false).await.unwrap();

        let encoded_wal_options = serde_json::to_string(&WalOptions::RaftEngine).unwrap();
        let expected = regions
            .into_iter()
            .zip(vec![encoded_wal_options; num_regions as usize])
            .collect();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn test_refuse_invalid_topic_name_prefix() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let wal_config = MetasrvWalConfig::Kafka(MetasrvKafkaConfig {
            kafka_topic: KafkaTopicConfig {
                topic_name_prefix: "``````".to_string(),
                ..Default::default()
            },
            ..Default::default()
        });
        let got = build_wal_provider(&wal_config, kv_backend)
            .await
            .unwrap_err();
        assert_matches!(got, Error::InvalidTopicNamePrefix { .. });
    }

    #[tokio::test]
    async fn test_provider_with_kafka_allocate_wal_options() {
        common_telemetry::init_default_ut_logging();
        maybe_skip_kafka_integration_test!();
        let num_topics = 5;
        let mut topic_pool = test_kafka_topic_pool(
            get_kafka_endpoints(),
            num_topics,
            true,
            Some("test_allocator_with_kafka"),
        )
        .await;
        topic_pool.selector = Arc::new(RoundRobinTopicSelector::default());
        let topics = topic_pool.topics.clone();
        // clean up the topics before test
        let topic_creator = topic_pool.topic_creator();
        topic_creator.delete_topics(&topics).await.unwrap();

        // Creates an options provider.
        let provider = WalProvider::Kafka(topic_pool);
        provider.start().await.unwrap();

        let num_regions = 3;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = provider.allocate(&regions, false).await.unwrap();

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
    }

    #[tokio::test]
    async fn test_provider_with_skip_wal() {
        let provider = WalProvider::RaftEngine;
        provider.start().await.unwrap();

        let num_regions = 32;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = provider.allocate(&regions, true).await.unwrap();
        assert_eq!(got.len(), num_regions as usize);
        for wal_options in got.values() {
            assert_eq!(wal_options, &"{\"wal.provider\":\"noop\"}");
        }
    }
}
