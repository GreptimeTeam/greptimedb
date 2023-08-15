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

//! Mito region.

pub(crate) mod opener;
pub(crate) mod version;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common_telemetry::info;
use store_api::storage::RegionId;

use crate::error::Result;
use crate::manifest::manager::RegionManifestManager;
use crate::metadata::RegionMetadataRef;
use crate::region::version::VersionControlRef;

/// Type to store region version.
pub type VersionNumber = u32;

/// Metadata and runtime status of a region.
#[derive(Debug)]
pub(crate) struct MitoRegion {
    /// Id of this region.
    ///
    /// Accessing region id from the version control is inconvenient so
    /// we also store it here.
    pub(crate) region_id: RegionId,

    /// Version controller for this region.
    pub(crate) version_control: VersionControlRef,
    /// Manager to maintain manifest for this region.
    manifest_manager: RegionManifestManager,
}

pub(crate) type MitoRegionRef = Arc<MitoRegion>;

impl MitoRegion {
    /// Stop background tasks for this region.
    pub(crate) async fn stop(&self) -> Result<()> {
        self.manifest_manager.stop().await?;

        info!("Stopped region, region_id: {}", self.region_id);

        Ok(())
    }

    /// Returns current metadata of the region.
    pub(crate) fn metadata(&self) -> RegionMetadataRef {
        let version_data = self.version_control.current();
        version_data.version.metadata.clone()
    }
}

/// Regions indexed by ids.
#[derive(Debug, Default)]
pub(crate) struct RegionMap {
    regions: RwLock<HashMap<RegionId, MitoRegionRef>>,
}

impl RegionMap {
    /// Returns true if the region exists.
    pub(crate) fn is_region_exists(&self, region_id: RegionId) -> bool {
        let regions = self.regions.read().unwrap();
        regions.contains_key(&region_id)
    }

    /// Inserts a new region into the map.
    pub(crate) fn insert_region(&self, region: MitoRegionRef) {
        let mut regions = self.regions.write().unwrap();
        regions.insert(region.region_id, region);
    }

    /// Get region by region id.
    pub(crate) fn get_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        let regions = self.regions.read().unwrap();
        regions.get(&region_id).cloned()
    }

    /// Remove region by id.
    pub(crate) fn remove_region(&self, region_id: RegionId) {
        let mut regions = self.regions.write().unwrap();
        regions.remove(&region_id);
    }

    /// List all regions.
    pub(crate) fn list_regions(&self) -> Vec<MitoRegionRef> {
        let regions = self.regions.read().unwrap();
        regions.values().cloned().collect()
    }

    /// Clear the map.
    pub(crate) fn clear(&self) {
        self.regions.write().unwrap().clear();
    }
}

pub(crate) type RegionMapRef = Arc<RegionMap>;
