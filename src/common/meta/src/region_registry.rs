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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common_telemetry::warn;
use store_api::storage::RegionId;

use crate::datanode::{RegionManifestInfo, RegionStat};

/// Represents information about a leader region in the cluster.
/// Contains the datanode id where the leader is located,
/// and the current manifest version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaderRegion {
    pub datanode_id: u64,
    pub manifest: LeaderRegionManifestInfo,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LeaderRegionManifestInfo {
    Mito {
        manifest_version: u64,
        flushed_entry_id: u64,
        topic_latest_entry_id: u64,
    },
    Metric {
        data_manifest_version: u64,
        data_flushed_entry_id: u64,
        data_topic_latest_entry_id: u64,
        metadata_manifest_version: u64,
        metadata_flushed_entry_id: u64,
        metadata_topic_latest_entry_id: u64,
    },
}

impl LeaderRegionManifestInfo {
    /// Generate a [LeaderRegionManifestInfo] from [RegionStat].
    pub fn from_region_stat(region_stat: &RegionStat) -> LeaderRegionManifestInfo {
        match region_stat.region_manifest {
            RegionManifestInfo::Metric {
                data_manifest_version,
                data_flushed_entry_id,
                metadata_manifest_version,
                metadata_flushed_entry_id,
            } => LeaderRegionManifestInfo::Metric {
                data_manifest_version,
                data_flushed_entry_id,
                data_topic_latest_entry_id: region_stat.data_topic_latest_entry_id,
                metadata_manifest_version,
                metadata_flushed_entry_id,
                metadata_topic_latest_entry_id: region_stat.metadata_topic_latest_entry_id,
            },
            RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
            } => LeaderRegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
                topic_latest_entry_id: region_stat.data_topic_latest_entry_id,
            },
        }
    }

    /// Returns the manifest version of the leader region.
    pub fn manifest_version(&self) -> u64 {
        match self {
            LeaderRegionManifestInfo::Mito {
                manifest_version, ..
            } => *manifest_version,
            LeaderRegionManifestInfo::Metric {
                data_manifest_version,
                ..
            } => *data_manifest_version,
        }
    }

    /// Returns the flushed entry id of the leader region.
    pub fn flushed_entry_id(&self) -> u64 {
        match self {
            LeaderRegionManifestInfo::Mito {
                flushed_entry_id, ..
            } => *flushed_entry_id,
            LeaderRegionManifestInfo::Metric {
                data_flushed_entry_id,
                ..
            } => *data_flushed_entry_id,
        }
    }

    /// Returns prunable entry id of the leader region.
    /// It is used to determine the entry id that can be pruned in remote wal.
    ///
    /// For a mito region, the prunable entry id should max(flushed_entry_id, latest_entry_id_since_flush).
    ///
    /// For a metric region, the prunable entry id should min(
    ///     max(data_flushed_entry_id, data_latest_entry_id_since_flush),
    ///     max(metadata_flushed_entry_id, metadata_latest_entry_id_since_flush)
    /// ).
    pub fn prunable_entry_id(&self) -> u64 {
        match self {
            LeaderRegionManifestInfo::Mito {
                flushed_entry_id,
                topic_latest_entry_id,
                ..
            } => (*flushed_entry_id).max(*topic_latest_entry_id),
            LeaderRegionManifestInfo::Metric {
                data_flushed_entry_id,
                data_topic_latest_entry_id,
                metadata_flushed_entry_id,
                metadata_topic_latest_entry_id,
                ..
            } => {
                let data_prunable_entry_id =
                    (*data_flushed_entry_id).max(*data_topic_latest_entry_id);
                let metadata_prunable_entry_id =
                    (*metadata_flushed_entry_id).max(*metadata_topic_latest_entry_id);
                data_prunable_entry_id.min(metadata_prunable_entry_id)
            }
        }
    }
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
        region_ids
            .into_iter()
            .flat_map(|region_id| {
                inner
                    .get(&region_id)
                    .map(|leader_region| (region_id, *leader_region))
            })
            .collect::<HashMap<_, _>>()
    }

    /// Puts the leader regions into the registry.
    pub fn batch_put(&self, key_values: Vec<(RegionId, LeaderRegion)>) {
        let mut inner = self.inner.write().unwrap();
        for (region_id, leader_region) in key_values {
            match inner.entry(region_id) {
                Entry::Vacant(entry) => {
                    entry.insert(leader_region);
                }
                Entry::Occupied(mut entry) => {
                    let manifest_version = entry.get().manifest.manifest_version();
                    if manifest_version > leader_region.manifest.manifest_version() {
                        warn!(
                            "Received a leader region with a smaller manifest version than the existing one, ignore it. region: {}, existing_manifest_version: {}, new_manifest_version: {}",
                            region_id,
                            manifest_version,
                            leader_region.manifest.manifest_version()
                        );
                    } else {
                        entry.insert(leader_region);
                    }
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
