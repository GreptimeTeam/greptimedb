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
pub mod options;
pub(crate) mod version;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::info;
use common_time::util::current_time_millis;
use snafu::{ensure, OptionExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::error::{RegionNotFoundSnafu, RegionReadonlySnafu, Result};
use crate::manifest::manager::RegionManifestManager;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::OnFailure;
use crate::sst::file_purger::FilePurgerRef;

/// This is the ratio of the size of memtables to the size of wal,
/// It is a rough estimation of the size of wal.
const ESTIMATED_WAL_FACTOR: f32 = 0.565642;

/// Metadata and runtime status of a region.
///
/// Writing and reading a region follow a single-writer-multi-reader rule:
/// - Only the region worker thread this region belongs to can modify the metadata.
/// - Multiple reader threads are allowed to read a specific `version` of a region.
#[derive(Debug)]
pub(crate) struct MitoRegion {
    /// Id of this region.
    ///
    /// Accessing region id from the version control is inconvenient so
    /// we also store it here.
    pub(crate) region_id: RegionId,

    /// Version controller for this region.
    pub(crate) version_control: VersionControlRef,
    /// SSTs accessor for this region.
    pub(crate) access_layer: AccessLayerRef,
    /// Manager to maintain manifest for this region.
    pub(crate) manifest_manager: RegionManifestManager,
    /// SST file purger.
    pub(crate) file_purger: FilePurgerRef,
    /// Last flush time in millis.
    last_flush_millis: AtomicI64,
    /// Whether the region is writable.
    writable: AtomicBool,
}

pub(crate) type MitoRegionRef = Arc<MitoRegion>;

impl MitoRegion {
    /// Stop background managers for this region.
    pub(crate) async fn stop(&self) -> Result<()> {
        self.manifest_manager.stop().await?;

        info!(
            "Stopped region manifest manager, region_id: {}",
            self.region_id
        );

        Ok(())
    }

    /// Returns current metadata of the region.
    pub(crate) fn metadata(&self) -> RegionMetadataRef {
        let version_data = self.version_control.current();
        version_data.version.metadata.clone()
    }

    /// Returns current version of the region.
    pub(crate) fn version(&self) -> VersionRef {
        let version_data = self.version_control.current();
        version_data.version
    }

    /// Returns last flush timestamp in millis.
    pub(crate) fn last_flush_millis(&self) -> i64 {
        self.last_flush_millis.load(Ordering::Relaxed)
    }

    /// Update flush time to current time.
    pub(crate) fn update_flush_millis(&self) {
        let now = current_time_millis();
        self.last_flush_millis.store(now, Ordering::Relaxed);
    }

    /// Returns whether the region is writable.
    pub(crate) fn is_writable(&self) -> bool {
        self.writable.load(Ordering::Relaxed)
    }

    /// Sets the writable flag.
    pub(crate) fn set_writable(&self, writable: bool) {
        self.writable.store(writable, Ordering::Relaxed);
    }

    /// Estimated WAL size in bytes.
    /// Use the size of memtables to estimate the size of wal.
    pub(crate) fn estimated_wal_size(&self) -> usize {
        let memtables = &self.version().memtables;
        let memtable_size = memtables.mutable_usage() + memtables.immutables_usage();
        ((memtable_size as f32) * ESTIMATED_WAL_FACTOR) as usize
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

    /// Gets region by region id.
    pub(crate) fn get_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        let regions = self.regions.read().unwrap();
        regions.get(&region_id).cloned()
    }

    /// Gets writable region by region id.
    ///
    /// Returns error if the region does not exist or is readonly.
    pub(crate) fn writable_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(region.is_writable(), RegionReadonlySnafu { region_id });
        Ok(region)
    }

    /// Gets writable region by region id.
    ///
    /// Calls the callback if the region does not exist or is readonly.
    pub(crate) fn writable_region_or<F: OnFailure>(
        &self,
        region_id: RegionId,
        cb: &mut F,
    ) -> Option<MitoRegionRef> {
        match self.writable_region(region_id) {
            Ok(region) => Some(region),
            Err(e) => {
                cb.on_failure(e);
                None
            }
        }
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
