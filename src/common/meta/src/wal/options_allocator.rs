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

use crate::error::Result;
use crate::kv_backend::KvBackendRef;
use crate::wal::kafka::topic_manager::TopicManager as KafkaTopicManager;
use crate::wal::{WalConfig, WalOptions};

#[derive(Default)]
pub struct WalOptionsAllocator {
    kafka_topic_manager: Option<KafkaTopicManager>,
}

pub type WalOptionsAllocatorRef = Arc<WalOptionsAllocator>;

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
        vec![WalOptions::default(); num_regions]
    }
}
