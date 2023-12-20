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

use std::collections::HashMap;
use std::sync::Arc;

use common_config::{KafkaWalOptions, WalOptions};
use snafu::ResultExt;
use store_api::storage::RegionNumber;

use crate::error::{EncodeWalOptionsToJsonSnafu, Result};
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::TopicManager as KafkaTopicManager;
use crate::wal::WalConfig;

/// Allocates wal options in region granularity.
#[derive(Default)]
pub enum WalOptionsAllocator {
    #[default]
    RaftEngine,
    Kafka(KafkaTopicManager),
}

impl WalOptionsAllocator {
    /// Creates a WalOptionsAllocator.
    pub fn new(config: WalConfig, kv_backend: KvBackendRef) -> Self {
        match config {
            WalConfig::RaftEngine => Self::RaftEngine,
            WalConfig::Kafka(kafka_config) => {
                Self::Kafka(KafkaTopicManager::new(kafka_config, kv_backend))
            }
        }
    }

    /// Tries to initialize the allocator.
    pub async fn try_init(&mut self) -> Result<()> {
        match self {
            Self::RaftEngine => Ok(()),
            Self::Kafka(kafka_topic_manager) => kafka_topic_manager.try_init().await,
        }
    }

    /// Allocates a wal options for a region.
    pub fn alloc(&self) -> WalOptions {
        match self {
            Self::RaftEngine => WalOptions::RaftEngine,
            Self::Kafka(kafka_topic_manager) => WalOptions::Kafka(KafkaWalOptions {
                topic: kafka_topic_manager.select().clone(),
            }),
        }
    }

    /// Allocates a batch of wal options where each wal options goes to a region.
    pub fn alloc_batch(&self, num_regions: usize) -> Vec<WalOptions> {
        match self {
            WalOptionsAllocator::RaftEngine => vec![WalOptions::RaftEngine; num_regions],
            WalOptionsAllocator::Kafka(topic_manager) => topic_manager
                .select_batch(num_regions)
                .into_iter()
                .map(|topic| {
                    WalOptions::Kafka(KafkaWalOptions {
                        topic: topic.clone(),
                    })
                })
                .collect(),
        }
    }
}

/// Creates and initializes a wal options allocator.
pub async fn build_wal_options_allocator(
    config: &WalConfig,
    kv_backend: &KvBackendRef,
) -> Result<WalOptionsAllocator> {
    let mut allocator = WalOptionsAllocator::new(config.clone(), kv_backend.clone());
    allocator.try_init().await?;
    Ok(allocator)
}

/// Allocates a wal options for each region. The allocated wal options is encoded immediately.
pub fn allocate_region_wal_options(
    regions: Vec<RegionNumber>,
    wal_options_allocator: &WalOptionsAllocator,
) -> Result<HashMap<RegionNumber, String>> {
    let wal_options = wal_options_allocator
        .alloc_batch(regions.len())
        .into_iter()
        .map(|wal_options| {
            serde_json::to_string(&wal_options).context(EncodeWalOptionsToJsonSnafu { wal_options })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(regions.into_iter().zip(wal_options).collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kv_backend::memory::MemoryKvBackend;

    // Tests the wal options allocator could successfully allocate raft-engine wal options.
    // Note: tests for allocator with kafka are integration tests.
    #[tokio::test]
    async fn test_allocator_with_raft_engine() {
        let kv_backend = Arc::new(MemoryKvBackend::new()) as KvBackendRef;
        let wal_config = WalConfig::RaftEngine;
        let allocator = build_wal_options_allocator(&wal_config, &kv_backend)
            .await
            .unwrap();

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
}
