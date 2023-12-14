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

use serde::{Deserialize, Serialize};
use store_api::storage::RegionNumber;

use crate::error::Result;
use crate::wal::kafka::{KafkaTopic, KafkaTopicManager};
use crate::wal::WalConfig;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct RegionWalOptions {
    pub kafka_topic: Option<KafkaTopic>,
}

pub type RegionWalOptionsMap = HashMap<RegionNumber, RegionWalOptions>;
pub type EncodedRegionWalOptions = HashMap<String, String>;

impl From<&RegionWalOptions> for EncodedRegionWalOptions {
    fn from(value: &RegionWalOptions) -> Self {
        // TODO(niebayes): implement encoding/decoding for region wal options.
        EncodedRegionWalOptions::default()
    }
}

impl TryFrom<&EncodedRegionWalOptions> for RegionWalOptions {
    type Error = crate::error::Error;

    fn try_from(value: &EncodedRegionWalOptions) -> Result<Self> {
        todo!()
    }
}

pub struct RegionWalOptionsAllocator {
    // TODO(niebayes): uncomment this.
    // kafka_topic_manager: KafkaTopicManager,
}

impl RegionWalOptionsAllocator {
    /// Creates a RegionWalOptionsAllocator.
    pub fn new(config: &WalConfig) -> Self {
        // TODO(niebayes): properly init.
        Self {}
    }

    pub fn try_init(&self) -> Result<()> {
        todo!()
    }

    /// Allocates a wal options for a region.
    pub fn alloc(&self) -> RegionWalOptions {
        todo!()
    }

    /// Allocates a wal options for each region.
    pub fn alloc_batch(&self, num_regions: usize) -> Vec<RegionWalOptions> {
        // TODO(niebayes): allocate a batch of region wal options.
        vec![RegionWalOptions::default(); num_regions]
    }
}
