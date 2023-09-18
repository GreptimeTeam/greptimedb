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

//! Options for a region.

use std::time::Duration;

use store_api::storage::CompactionStrategy;

/// Options that affect the entire region.
///
/// Users need to specify the options while creating/opening a region.
#[derive(Debug)]
pub struct RegionOptions {
    /// Region SST files TTL.
    pub ttl: Option<Duration>,
    /// Compaction strategy.
    pub compaction_strategy: CompactionStrategy,
}

impl Default for RegionOptions {
    fn default() -> Self {
        RegionOptions {
            ttl: None,
            compaction_strategy: CompactionStrategy::default(),
        }
    }
}
