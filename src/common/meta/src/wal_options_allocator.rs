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
use common_wal::options::{KafkaWalOptions, WalOptions, WAL_OPTIONS_KEY};
use snafu::{ensure, ResultExt};
use store_api::storage::{RegionId, RegionNumber};

use crate::error::{EncodeWalOptionsSnafu, InvalidTopicNamePrefixSnafu, Result};
use crate::key::NAME_PATTERN_REGEX;
use crate::kv_backend::KvBackendRef;
use crate::leadership_notifier::LeadershipChangeListener;
pub use crate::wal_options_allocator::topic_creator::{
    build_kafka_client, build_kafka_topic_creator,
};
use crate::wal_options_allocator::topic_pool::KafkaTopicPool;

/// Allocates wal options in region granularity.
#[derive(Default, Debug)]
pub enum WalOptionsAllocator {
    #[default]
    RaftEngine,
    Kafka(KafkaTopicPool),
}

/// Arc wrapper of WalOptionsAllocator.
pub type WalOptionsAllocatorRef = Arc<WalOptionsAllocator>;

impl WalOptionsAllocator {
    /// Tries to start the allocator.
    pub async fn start(&self) -> Result<()> {
        match self {
            Self::RaftEngine => Ok(()),
            Self::Kafka(kafka_topic_manager) => kafka_topic_manager.activate().await,
        }
    }

    /// Allocates a batch of wal options where each wal options goes to a region.
    /// If skip_wal is true, the wal options will be set to Noop regardless of the allocator type.
    pub fn alloc_batch(&self, num_regions: usize, skip_wal: bool) -> Result<Vec<WalOptions>> {
        if skip_wal {
            return Ok(vec![WalOptions::Noop; num_regions]);
        }
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

/// Builds a wal options allocator based on the given configuration.
pub async fn build_wal_options_allocator(
    config: &MetasrvWalConfig,
    kv_backend: KvBackendRef,
) -> Result<WalOptionsAllocator> {
    match config {
        MetasrvWalConfig::RaftEngine => Ok(WalOptionsAllocator::RaftEngine),
        MetasrvWalConfig::Kafka(kafka_config) => {
            let prefix = &kafka_config.kafka_topic.topic_name_prefix;
            ensure!(
                NAME_PATTERN_REGEX.is_match(prefix),
                InvalidTopicNamePrefixSnafu { prefix }
            );
            let topic_creator =
                build_kafka_topic_creator(&kafka_config.connection, &kafka_config.kafka_topic)
                    .await?;
            let topic_pool = KafkaTopicPool::new(kafka_config, kv_backend, topic_creator);
            Ok(WalOptionsAllocator::Kafka(topic_pool))
        }
    }
}

/// Allocates a wal options for each region. The allocated wal options is encoded immediately.
pub fn allocate_region_wal_options(
    regions: Vec<RegionNumber>,
    wal_options_allocator: &WalOptionsAllocator,
    skip_wal: bool,
) -> Result<HashMap<RegionNumber, String>> {
    let wal_options = wal_options_allocator
        .alloc_batch(regions.len(), skip_wal)?
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
    use std::assert_matches::assert_matches;

    use common_wal::config::kafka::common::KafkaTopicConfig;
    use common_wal::config::kafka::MetasrvKafkaConfig;
    use common_wal::maybe_skip_kafka_integration_test;
    use common_wal::test_util::get_kafka_endpoints;

    use super::*;
    use crate::error::Error;
    use crate::kv_backend::memory::MemoryKvBackend;
    use crate::test_util::test_kafka_topic_pool;
    use crate::wal_options_allocator::selector::RoundRobinTopicSelector;

    // Tests that the wal options allocator could successfully allocate raft-engine wal options.
    #[tokio::test]
    async fn test_allocator_with_raft_engine() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let wal_config = MetasrvWalConfig::RaftEngine;
        let allocator = build_wal_options_allocator(&wal_config, kv_backend)
            .await
            .unwrap();
        allocator.start().await.unwrap();

        let num_regions = 32;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = allocate_region_wal_options(regions.clone(), &allocator, false).unwrap();

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
        let got = build_wal_options_allocator(&wal_config, kv_backend)
            .await
            .unwrap_err();
        assert_matches!(got, Error::InvalidTopicNamePrefix { .. });
    }

    #[tokio::test]
    async fn test_allocator_with_kafka_allocate_wal_options() {
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

        // Creates an options allocator.
        let allocator = WalOptionsAllocator::Kafka(topic_pool);
        allocator.start().await.unwrap();

        let num_regions = 3;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = allocate_region_wal_options(regions.clone(), &allocator, false).unwrap();

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
    async fn test_allocator_with_skip_wal() {
        let allocator = WalOptionsAllocator::RaftEngine;
        allocator.start().await.unwrap();

        let num_regions = 32;
        let regions = (0..num_regions).collect::<Vec<_>>();
        let got = allocate_region_wal_options(regions.clone(), &allocator, true).unwrap();
        assert_eq!(got.len(), num_regions as usize);
        for wal_options in got.values() {
            assert_eq!(wal_options, &"{\"wal.provider\":\"noop\"}");
        }
    }
}
