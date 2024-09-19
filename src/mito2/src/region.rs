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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::{error, info, warn};
use crossbeam_utils::atomic::AtomicCell;
use snafu::{ensure, OptionExt};
use store_api::logstore::provider::Provider;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::error::{RegionNotFoundSnafu, RegionStateSnafu, RegionTruncatedSnafu, Result};
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::RegionManifestManager;
use crate::memtable::MemtableBuilderRef;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::{OnFailure, OptionOutputTx};
use crate::sst::file_purger::FilePurgerRef;
use crate::time_provider::TimeProviderRef;

/// This is the approximate factor to estimate the size of wal.
const ESTIMATED_WAL_FACTOR: f32 = 0.42825;

/// Region status include region id, memtable usage, sst usage, wal usage and manifest usage.
#[derive(Debug)]
pub struct RegionUsage {
    pub region_id: RegionId,
    pub wal_usage: u64,
    pub sst_usage: u64,
    pub manifest_usage: u64,
}

impl RegionUsage {
    pub fn disk_usage(&self) -> u64 {
        self.wal_usage + self.sst_usage + self.manifest_usage
    }
}

/// State of the region.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionState {
    /// The region is opened but is still read-only.
    ReadOnly,
    /// The region is opened and is writable.
    Writable,
    /// The region is altering.
    Altering,
    /// The region is dropping.
    Dropping,
    /// The region is truncating.
    Truncating,
    /// The region is handling a region edit.
    Editing,
}

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
    ///
    /// We MUST update the version control inside the write lock of the region manifest manager.
    pub(crate) version_control: VersionControlRef,
    /// SSTs accessor for this region.
    pub(crate) access_layer: AccessLayerRef,
    /// Context to maintain manifest for this region.
    pub(crate) manifest_ctx: ManifestContextRef,
    /// SST file purger.
    pub(crate) file_purger: FilePurgerRef,
    /// The provider of log store.
    pub(crate) provider: Provider,
    /// Last flush time in millis.
    last_flush_millis: AtomicI64,
    /// Last compaction time in millis.
    last_compaction_millis: AtomicI64,
    /// Provider to get current time.
    time_provider: TimeProviderRef,
    /// Memtable builder for the region.
    pub(crate) memtable_builder: MemtableBuilderRef,
    /// manifest stats
    stats: ManifestStats,
}

pub(crate) type MitoRegionRef = Arc<MitoRegion>;

impl MitoRegion {
    /// Stop background managers for this region.
    pub(crate) async fn stop(&self) {
        self.manifest_ctx
            .manifest_manager
            .write()
            .await
            .stop()
            .await;

        info!(
            "Stopped region manifest manager, region_id: {}",
            self.region_id
        );
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
        let now = self.time_provider.current_time_millis();
        self.last_flush_millis.store(now, Ordering::Relaxed);
    }

    /// Return last compaction time in millis.
    pub(crate) fn last_compaction_millis(&self) -> i64 {
        self.last_compaction_millis.load(Ordering::Relaxed)
    }

    /// Update compaction time to now millis.
    pub(crate) fn update_compaction_millis(&self) {
        let now = self.time_provider.current_time_millis();
        self.last_compaction_millis.store(now, Ordering::Relaxed);
    }

    /// Returns the region dir.
    pub(crate) fn region_dir(&self) -> &str {
        self.access_layer.region_dir()
    }

    /// Returns whether the region is writable.
    pub(crate) fn is_writable(&self) -> bool {
        self.manifest_ctx.state.load() == RegionState::Writable
    }

    /// Returns whether the region is readonly.
    pub(crate) fn is_readonly(&self) -> bool {
        self.manifest_ctx.state.load() == RegionState::ReadOnly
    }

    /// Returns the state of the region.
    pub(crate) fn state(&self) -> RegionState {
        self.manifest_ctx.state.load()
    }

    /// Sets the writable state.
    pub(crate) fn set_writable(&self, writable: bool) {
        if writable {
            // Only sets the region to writable if it is read only.
            // This prevents others updating the manifest.
            match self
                .manifest_ctx
                .state
                .compare_exchange(RegionState::ReadOnly, RegionState::Writable)
            {
                Ok(state) => info!(
                    "Set region {} to writable, previous state: {:?}",
                    self.region_id, state
                ),
                Err(state) => {
                    if state != RegionState::Writable {
                        warn!(
                            "Failed to set region {} to writable, current state: {:?}",
                            self.region_id, state
                        )
                    }
                }
            }
        } else {
            self.manifest_ctx.state.store(RegionState::ReadOnly);
        }
    }

    /// Sets the altering state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_altering(&self) -> Result<()> {
        self.compare_exchange_state(RegionState::Writable, RegionState::Altering)
    }

    /// Sets the dropping state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_dropping(&self) -> Result<()> {
        self.compare_exchange_state(RegionState::Writable, RegionState::Dropping)
    }

    /// Sets the truncating state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_truncating(&self) -> Result<()> {
        self.compare_exchange_state(RegionState::Writable, RegionState::Truncating)
    }

    /// Sets the editing state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_editing(&self) -> Result<()> {
        self.compare_exchange_state(RegionState::Writable, RegionState::Editing)
    }

    /// Sets the region to readonly gracefully. This acquires the manifest write lock.
    pub(crate) async fn set_readonly_gracefully(&self) {
        let _manager = self.manifest_ctx.manifest_manager.write().await;
        // We acquires the write lock of the manifest manager to ensure that no one is updating the manifest.
        // Then we change the state.
        self.set_writable(false);
    }

    /// Switches the region state to `RegionState::Writable` if the current state is `expect`.
    /// Otherwise, logs an error.
    pub(crate) fn switch_state_to_writable(&self, expect: RegionState) {
        if let Err(e) = self.compare_exchange_state(expect, RegionState::Writable) {
            error!(e; "failed to switch region state to writable, expect state is {:?}", expect);
        }
    }

    /// Returns the region usage in bytes.
    pub(crate) fn region_usage(&self) -> RegionUsage {
        let region_id = self.region_id;

        let version = self.version();
        let memtables = &version.memtables;
        let memtable_usage = (memtables.mutable_usage() + memtables.immutables_usage()) as u64;

        let sst_usage = version.ssts.sst_usage();

        let wal_usage = self.estimated_wal_usage(memtable_usage);
        let manifest_usage = self.stats.total_manifest_size();

        RegionUsage {
            region_id,
            wal_usage,
            sst_usage,
            manifest_usage,
        }
    }

    /// Estimated WAL size in bytes.
    /// Use the memtables size to estimate the size of wal.
    fn estimated_wal_usage(&self, memtable_usage: u64) -> u64 {
        ((memtable_usage as f32) * ESTIMATED_WAL_FACTOR) as u64
    }

    /// Sets the state of the region to given state if the current state equals to
    /// the expected.
    fn compare_exchange_state(&self, expect: RegionState, state: RegionState) -> Result<()> {
        self.manifest_ctx
            .state
            .compare_exchange(expect, state)
            .map_err(|actual| {
                RegionStateSnafu {
                    region_id: self.region_id,
                    state: actual,
                    expect,
                }
                .build()
            })?;
        Ok(())
    }
}

/// Context to update the region manifest.
#[derive(Debug)]
pub(crate) struct ManifestContext {
    /// Manager to maintain manifest for this region.
    manifest_manager: tokio::sync::RwLock<RegionManifestManager>,
    /// The state of the region. The region checks the state before updating
    /// manifest.
    state: AtomicCell<RegionState>,
}

impl ManifestContext {
    pub(crate) fn new(manager: RegionManifestManager, state: RegionState) -> Self {
        ManifestContext {
            manifest_manager: tokio::sync::RwLock::new(manager),
            state: AtomicCell::new(state),
        }
    }

    pub(crate) async fn has_update(&self) -> Result<bool> {
        self.manifest_manager.read().await.has_update().await
    }

    /// Updates the manifest if current state is `expect_state`.
    pub(crate) async fn update_manifest(
        &self,
        expect_state: RegionState,
        action_list: RegionMetaActionList,
    ) -> Result<()> {
        // Acquires the write lock of the manifest manager.
        let mut manager = self.manifest_manager.write().await;
        // Gets current manifest.
        let manifest = manager.manifest();
        // Checks state inside the lock. This is to ensure that we won't update the manifest
        // after `set_readonly_gracefully()` is called.
        let current_state = self.state.load();
        ensure!(
            current_state == expect_state,
            RegionStateSnafu {
                region_id: manifest.metadata.region_id,
                state: current_state,
                expect: expect_state,
            }
        );

        for action in &action_list.actions {
            // Checks whether the edit is still applicable.
            let RegionMetaAction::Edit(edit) = &action else {
                continue;
            };

            // Checks whether the region is truncated.
            let Some(truncated_entry_id) = manifest.truncated_entry_id else {
                continue;
            };

            // This is an edit from flush.
            if let Some(flushed_entry_id) = edit.flushed_entry_id {
                ensure!(
                    truncated_entry_id < flushed_entry_id,
                    RegionTruncatedSnafu {
                        region_id: manifest.metadata.region_id,
                    }
                );
            }

            // This is an edit from compaction.
            if !edit.files_to_remove.is_empty() {
                // Input files of the compaction task has been truncated.
                for file in &edit.files_to_remove {
                    ensure!(
                        manifest.files.contains_key(&file.file_id),
                        RegionTruncatedSnafu {
                            region_id: manifest.metadata.region_id,
                        }
                    );
                }
            }
        }

        // Now we can update the manifest.
        manager.update(action_list).await.inspect_err(
            |e| error!(e; "Failed to update manifest, region_id: {}", manifest.metadata.region_id),
        )?;

        if self.state.load() == RegionState::ReadOnly {
            warn!(
                "Region {} becomes read-only while updating manifest which may cause inconsistency",
                manifest.metadata.region_id
            );
        }

        Ok(())
    }
}

#[cfg(test)]
impl ManifestContext {
    pub(crate) async fn manifest(&self) -> Arc<crate::manifest::action::RegionManifest> {
        self.manifest_manager.read().await.manifest()
    }
}

pub(crate) type ManifestContextRef = Arc<ManifestContext>;

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
        ensure!(
            region.is_writable(),
            RegionStateSnafu {
                region_id,
                state: region.state(),
                expect: RegionState::Writable,
            }
        );
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

/// Opening regions
#[derive(Debug, Default)]
pub(crate) struct OpeningRegions {
    regions: RwLock<HashMap<RegionId, Vec<OptionOutputTx>>>,
}

impl OpeningRegions {
    /// Registers `sender` for an opening region; Otherwise, it returns `None`.
    pub(crate) fn wait_for_opening_region(
        &self,
        region_id: RegionId,
        sender: OptionOutputTx,
    ) -> Option<OptionOutputTx> {
        let mut regions = self.regions.write().unwrap();
        match regions.entry(region_id) {
            Entry::Occupied(mut senders) => {
                senders.get_mut().push(sender);
                None
            }
            Entry::Vacant(_) => Some(sender),
        }
    }

    /// Returns true if the region exists.
    pub(crate) fn is_region_exists(&self, region_id: RegionId) -> bool {
        let regions = self.regions.read().unwrap();
        regions.contains_key(&region_id)
    }

    /// Inserts a new region into the map.
    pub(crate) fn insert_sender(&self, region: RegionId, sender: OptionOutputTx) {
        let mut regions = self.regions.write().unwrap();
        regions.insert(region, vec![sender]);
    }

    /// Remove region by id.
    pub(crate) fn remove_sender(&self, region_id: RegionId) -> Vec<OptionOutputTx> {
        let mut regions = self.regions.write().unwrap();
        regions.remove(&region_id).unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn sender_len(&self, region_id: RegionId) -> usize {
        let regions = self.regions.read().unwrap();
        if let Some(senders) = regions.get(&region_id) {
            senders.len()
        } else {
            0
        }
    }
}

pub(crate) type OpeningRegionsRef = Arc<OpeningRegions>;

/// Manifest stats.
#[derive(Default, Debug, Clone)]
pub(crate) struct ManifestStats {
    total_manifest_size: Arc<AtomicU64>,
}

impl ManifestStats {
    fn total_manifest_size(&self) -> u64 {
        self.total_manifest_size.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use crossbeam_utils::atomic::AtomicCell;

    use crate::region::RegionState;

    #[test]
    fn test_region_state_lock_free() {
        assert!(AtomicCell::<RegionState>::is_lock_free());
    }
}
