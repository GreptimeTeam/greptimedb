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
use std::sync::atomic::{AtomicI64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::{error, info};
use common_wal::options::WalOptions;
use snafu::{ensure, OptionExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::error::{RegionNotFoundSnafu, RegionStateSnafu, RegionTruncatedSnafu, Result};
use crate::manifest::action::{RegionMetaAction, RegionMetaActionList};
use crate::manifest::manager::RegionManifestManager;
use crate::memtable::MemtableBuilderRef;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::OnFailure;
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

// States of the region.
/// The region is opened but is still read-only.
pub(crate) const REGION_STATE_READ_ONLY: u8 = 0;
/// The region is opened and is writable.
pub(crate) const REGION_STATE_WRITABLE: u8 = 1;
/// The region is altering.
pub(crate) const REGION_STATE_ALTERING: u8 = 2;
/// The region is dropping.
pub(crate) const REGION_STATE_DROPPING: u8 = 3;
/// The region is truncating.
pub(crate) const REGION_STATE_TRUNCATING: u8 = 4;
/// The region is handling a region edit.
pub(crate) const REGION_STATE_EDITING: u8 = 5;

/// Returns a string representation of the region state.
pub(crate) fn region_state_to_str(state: u8) -> &'static str {
    match state {
        REGION_STATE_READ_ONLY => "READ_ONLY",
        REGION_STATE_WRITABLE => "WRITABLE",
        REGION_STATE_ALTERING => "ALTERING",
        REGION_STATE_DROPPING => "DROPPING",
        REGION_STATE_TRUNCATING => "TRUNCATING",
        REGION_STATE_EDITING => "EDITING",
        _ => "UNKNOWN",
    }
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
    /// Wal options of this region.
    pub(crate) wal_options: WalOptions,
    /// Last flush time in millis.
    last_flush_millis: AtomicI64,
    /// Provider to get current time.
    time_provider: TimeProviderRef,
    /// Memtable builder for the region.
    pub(crate) memtable_builder: MemtableBuilderRef,
}

pub(crate) type MitoRegionRef = Arc<MitoRegion>;

impl MitoRegion {
    /// Stop background managers for this region.
    pub(crate) async fn stop(&self) -> Result<()> {
        self.manifest_ctx
            .manifest_manager
            .write()
            .await
            .stop()
            .await?;

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
        let now = self.time_provider.current_time_millis();
        self.last_flush_millis.store(now, Ordering::Relaxed);
    }

    /// Returns the region dir.
    pub(crate) fn region_dir(&self) -> &str {
        self.access_layer.region_dir()
    }

    /// Returns whether the region is writable.
    pub(crate) fn is_writable(&self) -> bool {
        self.manifest_ctx.state.load(Ordering::Relaxed) == REGION_STATE_WRITABLE
    }

    /// Returns the state of the region.
    pub(crate) fn state(&self) -> u8 {
        self.manifest_ctx.state.load(Ordering::Relaxed)
    }

    /// Sets the writable state.
    pub(crate) fn set_writable(&self, writable: bool) {
        if writable {
            // Only sets the region to writable if it is read only.
            // This prevents others updating the manifest.
            let _ = self.manifest_ctx.state.compare_exchange(
                REGION_STATE_READ_ONLY,
                REGION_STATE_WRITABLE,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        } else {
            self.manifest_ctx
                .state
                .store(REGION_STATE_READ_ONLY, Ordering::Relaxed);
        }
    }

    /// Sets the altering state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_altering(&self) -> Result<()> {
        self.compare_exchange_state(REGION_STATE_WRITABLE, REGION_STATE_ALTERING)
    }

    /// Sets the dropping state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_dropping(&self) -> Result<()> {
        self.compare_exchange_state(REGION_STATE_WRITABLE, REGION_STATE_DROPPING)
    }

    /// Sets the truncating state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_truncating(&self) -> Result<()> {
        self.compare_exchange_state(REGION_STATE_WRITABLE, REGION_STATE_TRUNCATING)
    }

    /// Sets the editing state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_editing(&self) -> Result<()> {
        self.compare_exchange_state(REGION_STATE_WRITABLE, REGION_STATE_EDITING)
    }

    /// Sets the state of the region to given state if the current state equals to
    /// the expected.
    pub(crate) fn compare_exchange_state(&self, expect: u8, state: u8) -> Result<()> {
        self.manifest_ctx
            .state
            .compare_exchange(expect, state, Ordering::Relaxed, Ordering::Relaxed)
            .map_err(|actual| {
                RegionStateSnafu {
                    region_id: self.region_id,
                    state: actual,
                }
                .build()
            })?;
        Ok(())
    }

    /// Returns the region usage in bytes.
    pub(crate) async fn region_usage(&self) -> RegionUsage {
        let region_id = self.region_id;

        let version = self.version();
        let memtables = &version.memtables;
        let memtable_usage = (memtables.mutable_usage() + memtables.immutables_usage()) as u64;

        let sst_usage = version.ssts.sst_usage();

        let wal_usage = self.estimated_wal_usage(memtable_usage);

        let manifest_usage = self
            .manifest_ctx
            .manifest_manager
            .read()
            .await
            .manifest_usage();

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
}

/// Context to update the region manifest.
#[derive(Debug)]
pub(crate) struct ManifestContext {
    /// Manager to maintain manifest for this region.
    manifest_manager: tokio::sync::RwLock<RegionManifestManager>,
    /// The state of the region. The region checks the state before updating
    /// manifest.
    state: AtomicU8,
}

impl ManifestContext {
    pub(crate) fn new(manager: RegionManifestManager, state: u8) -> Self {
        ManifestContext {
            manifest_manager: tokio::sync::RwLock::new(manager),
            state: AtomicU8::new(state),
        }
    }

    pub(crate) async fn has_update(&self) -> Result<bool> {
        self.manifest_manager.read().await.has_update().await
    }

    // TODO(yingwen): checks the state.
    /// Updates the manifest and execute the `applier` if the manifest is updated.
    pub(crate) async fn update_manifest(
        &self,
        action_list: RegionMetaActionList,
        applier: impl FnOnce(),
    ) -> Result<()> {
        // Acquires the write lock of the manifest manager.
        let mut manager = self.manifest_manager.write().await;
        // Gets current manifest.
        let manifest = manager.manifest();

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

        // Executes the applier. We MUST holds the write lock.
        applier();

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

pub(crate) fn switch_state_to_writable(region: &MitoRegionRef, expect: u8) {
    if let Err(e) = region.compare_exchange_state(expect, REGION_STATE_WRITABLE) {
        error!(e; "failed to switch region state to writable, expect state is {}", region_state_to_str(expect));
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
        ensure!(
            region.is_writable(),
            RegionStateSnafu {
                region_id,
                state: region.state()
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
