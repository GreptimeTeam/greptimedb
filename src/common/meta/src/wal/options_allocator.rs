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

use crate::error::Result;
use crate::wal::{WalConfig, WalOptions};

pub struct WalOptionsAllocator {
    // TODO(niebayes): uncomment this.
    // kafka_topic_manager: KafkaTopicManager,
}

impl WalOptionsAllocator {
    /// Creates a WalOptionsAllocator.
    pub fn new(config: &WalConfig) -> Self {
        // TODO(niebayes): properly init.
        Self {}
    }

    pub fn try_init(&self) -> Result<()> {
        todo!()
    }

    /// Allocates a wal options for a region.
    pub fn alloc(&self) -> WalOptions {
        todo!()
    }

    /// Allocates a wal options for each region.
    pub fn alloc_batch(&self, num_regions: usize) -> Vec<WalOptions> {
        // TODO(niebayes): allocate a batch of region wal options.
        vec![WalOptions::default(); num_regions]
    }
}
