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
use std::sync::{Arc, RwLock};

use common_telemetry::warn;
use store_api::storage::RegionId;

/// Represents information about a leader region in the cluster.
/// Contains the datanode id where the leader is located,
/// and the current manifest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaderRegion {
    pub datanode_id: u64,
    pub manifest_version: u64,
}

pub type LeaderRegionRegistryRef = Arc<LeaderRegionRegistry>;

/// Registry that maintains a mapping of all leader regions in the cluster.
/// Tracks which datanode is hosting the leader for each region and the corresponding
/// manifest version.
#[derive(Default)]
pub struct LeaderRegionRegistry {
    inner: RwLock<HashMap<RegionId, LeaderRegion>>,
}

impl LeaderRegionRegistry {
    /// Creates a new empty leader region registry.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    /// Gets the leader region for the given region ids.
    pub fn batch_get<I: Iterator<Item = RegionId>>(
        &self,
        region_ids: I,
    ) -> HashMap<RegionId, LeaderRegion> {
        let inner = self.inner.read().unwrap();
        let mut result = HashMap::new();
        for region_id in region_ids {
            if let Some(leader_region) = inner.get(&region_id) {
                result.insert(region_id, *leader_region);
            }
        }
        result
    }

    /// Puts the leader regions into the registry.
    pub fn batch_put(&self, key_values: Vec<(RegionId, LeaderRegion)>) {
        let mut inner = self.inner.write().unwrap();
        for (region_id, leader_region) in key_values {
            if let Some(previous) = inner.insert(region_id, leader_region) {
                if previous.manifest_version > leader_region.manifest_version {
                    warn!(
                        "The manifest version of region {} is decreased from {} to {}",
                        region_id, previous.manifest_version, leader_region.manifest_version
                    );
                }
            }
        }
    }

    pub fn batch_delete<I: Iterator<Item = RegionId>>(&self, region_ids: I) {
        let mut inner = self.inner.write().unwrap();
        for region_id in region_ids {
            inner.remove(&region_id);
        }
    }

    /// Resets the registry to an empty state.
    pub fn reset(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.clear();
    }
}
