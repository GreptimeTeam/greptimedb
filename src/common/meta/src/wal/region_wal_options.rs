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

use crate::error::Result;
use crate::wal::kafka::Topic as KafkaTopic;
use crate::wal::WalOptions;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct RegionWalOptions;

impl RegionWalOptions {
    /// Tries to get the wal kafka topic from the options.
    pub fn kafka_topic(&self) -> Option<KafkaTopic> {
        todo!()
    }
}

pub type EncodedRegionWalOptions = HashMap<String, String>;

impl From<&RegionWalOptions> for EncodedRegionWalOptions {
    fn from(value: &RegionWalOptions) -> Self {
        todo!()
    }
}

impl TryFrom<&EncodedRegionWalOptions> for RegionWalOptions {
    type Error = crate::error::Error;

    fn try_from(value: &EncodedRegionWalOptions) -> Result<Self> {
        todo!()
    }
}

pub struct RegionWalOptionsAllocator;

impl RegionWalOptionsAllocator {
    /// Tries to create a RegionWalOptionsAllocator.
    pub fn try_new(wal_opts: &WalOptions) -> Result<Self> {
        todo!()
    }

    /// Allocates wal options for a region.
    pub fn alloc(&self) -> RegionWalOptions {
        todo!()
    }

    /// Allocates a batch of wal options for regions.
    pub fn alloc_batch(&self, num_regions: usize) -> Vec<RegionWalOptions> {
        todo!()
    }
}
