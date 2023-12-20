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
    pub fn new(config: &WalConfig, kv_backend: KvBackendRef) -> Self {
        todo!()
    }

    /// Tries to initialize the allocator.
    pub fn try_init(&self) -> Result<()> {
        todo!()
    }

    /// Allocates a wal options for a region.
    pub fn alloc(&self) -> WalOptions {
        todo!()
    }

    /// Allocates a batch of wal options where each wal options goes to a region.
    pub fn alloc_batch(&self, num_regions: usize) -> Vec<WalOptions> {
        match self {
            WalOptionsAllocator::RaftEngine => vec![WalOptions::RaftEngine; num_regions],
            WalOptionsAllocator::Kafka(topic_manager) => {
                let topics = topic_manager.select_batch(num_regions);
                topics
                    .into_iter()
                    .map(|topic| WalOptions::Kafka(KafkaWalOptions { topic }))
                    .collect()
            }
        }
    }
}

/// Allocates a wal options for each region. The allocated wal options is encoded immediately.
pub fn build_region_wal_options(
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

/// Builds a wal options allocator.
// TODO(niebayes): implement.
pub async fn build_wal_options_allocator(
    config: &WalConfig,
    kv_backend: &KvBackendRef,
) -> Result<WalOptionsAllocator> {
    let _ = config;
    let _ = kv_backend;
    Ok(WalOptionsAllocator::default())
}
