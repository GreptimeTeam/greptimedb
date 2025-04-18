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

pub mod opener;
pub mod options;
pub(crate) mod version;

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use common_telemetry::{error, info, warn};
use crossbeam_utils::atomic::AtomicCell;
use snafu::{ensure, OptionExt};
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::provider::Provider;
use store_api::manifest::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    RegionManifestInfo, RegionRole, RegionStatistic, SettableRegionRoleState,
};
use store_api::storage::RegionId;

use crate::access_layer::AccessLayerRef;
use crate::error::{
    FlushableRegionStateSnafu, RegionNotFoundSnafu, RegionStateSnafu, RegionTruncatedSnafu, Result,
    UpdateManifestSnafu,
};
use crate::manifest::action::{RegionManifest, RegionMetaAction, RegionMetaActionList};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionLeaderState {
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
    /// The region is stepping down.
    Downgrading,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionRoleState {
    Leader(RegionLeaderState),
    Follower,
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
    /// The topic's latest entry id since the region's last flushing.
    /// **Only used for remote WAL pruning.**
    ///
    /// The value will be updated to the high watermark of the topic
    /// if region receives a flush request or schedules a periodic flush task
    /// and the region's memtable is empty.    
    ///
    /// There are no WAL entries in range [flushed_entry_id, topic_latest_entry_id] for current region,
    /// which means these WAL entries maybe able to be pruned up to `topic_latest_entry_id`.
    pub(crate) topic_latest_entry_id: AtomicU64,
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

    /// Returns primary key encoding of the region.
    pub(crate) fn primary_key_encoding(&self) -> PrimaryKeyEncoding {
        let version_data = self.version_control.current();
        version_data.version.metadata.primary_key_encoding
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
        self.manifest_ctx.state.load() == RegionRoleState::Leader(RegionLeaderState::Writable)
    }

    /// Returns whether the region is flushable.
    pub(crate) fn is_flushable(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
                | RegionRoleState::Leader(RegionLeaderState::Downgrading)
        )
    }

    /// Returns whether the region is downgrading.
    pub(crate) fn is_downgrading(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        )
    }

    /// Returns whether the region is readonly.
    pub(crate) fn is_follower(&self) -> bool {
        self.manifest_ctx.state.load() == RegionRoleState::Follower
    }

    /// Returns the state of the region.
    pub(crate) fn state(&self) -> RegionRoleState {
        self.manifest_ctx.state.load()
    }

    /// Sets the region role state.
    pub(crate) fn set_role(&self, next_role: RegionRole) {
        self.manifest_ctx.set_role(next_role, self.region_id);
    }

    /// Sets the altering state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_altering(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::Altering),
        )
    }

    /// Sets the dropping state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_dropping(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::Dropping),
        )
    }

    /// Sets the truncating state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_truncating(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::Truncating),
        )
    }

    /// Sets the editing state.
    /// You should call this method in the worker loop.
    pub(crate) fn set_editing(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::Editing),
        )
    }

    /// Sets the region to readonly gracefully. This acquires the manifest write lock.
    pub(crate) async fn set_role_state_gracefully(&self, state: SettableRegionRoleState) {
        let _manager = self.manifest_ctx.manifest_manager.write().await;
        // We acquires the write lock of the manifest manager to ensure that no one is updating the manifest.
        // Then we change the state.
        self.set_role(state.into());
    }

    /// Switches the region state to `RegionRoleState::Leader(RegionLeaderState::Writable)` if the current state is `expect`.
    /// Otherwise, logs an error.
    pub(crate) fn switch_state_to_writable(&self, expect: RegionLeaderState) {
        if let Err(e) = self
            .compare_exchange_state(expect, RegionRoleState::Leader(RegionLeaderState::Writable))
        {
            error!(e; "failed to switch region state to writable, expect state is {:?}", expect);
        }
    }

    /// Returns the region statistic.
    pub(crate) fn region_statistic(&self) -> RegionStatistic {
        let version = self.version();
        let memtables = &version.memtables;
        let memtable_usage = (memtables.mutable_usage() + memtables.immutables_usage()) as u64;

        let sst_usage = version.ssts.sst_usage();
        let index_usage = version.ssts.index_usage();
        let flushed_entry_id = version.flushed_entry_id;

        let wal_usage = self.estimated_wal_usage(memtable_usage);
        let manifest_usage = self.stats.total_manifest_size();
        let num_rows = version.ssts.num_rows() + version.memtables.num_rows();
        let manifest_version = self.stats.manifest_version();

        let topic_latest_entry_id = self.topic_latest_entry_id.load(Ordering::Relaxed);

        RegionStatistic {
            num_rows,
            memtable_size: memtable_usage,
            wal_size: wal_usage,
            manifest_size: manifest_usage,
            sst_size: sst_usage,
            index_size: index_usage,
            manifest: RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
            },
            data_topic_latest_entry_id: topic_latest_entry_id,
            metadata_topic_latest_entry_id: topic_latest_entry_id,
        }
    }

    /// Estimated WAL size in bytes.
    /// Use the memtables size to estimate the size of wal.
    fn estimated_wal_usage(&self, memtable_usage: u64) -> u64 {
        ((memtable_usage as f32) * ESTIMATED_WAL_FACTOR) as u64
    }

    /// Sets the state of the region to given state if the current state equals to
    /// the expected.
    fn compare_exchange_state(
        &self,
        expect: RegionLeaderState,
        state: RegionRoleState,
    ) -> Result<()> {
        self.manifest_ctx
            .state
            .compare_exchange(RegionRoleState::Leader(expect), state)
            .map_err(|actual| {
                RegionStateSnafu {
                    region_id: self.region_id,
                    state: actual,
                    expect: RegionRoleState::Leader(expect),
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
    state: AtomicCell<RegionRoleState>,
}

impl ManifestContext {
    pub(crate) fn new(manager: RegionManifestManager, state: RegionRoleState) -> Self {
        ManifestContext {
            manifest_manager: tokio::sync::RwLock::new(manager),
            state: AtomicCell::new(state),
        }
    }

    pub(crate) async fn manifest_version(&self) -> ManifestVersion {
        self.manifest_manager
            .read()
            .await
            .manifest()
            .manifest_version
    }

    pub(crate) async fn has_update(&self) -> Result<bool> {
        self.manifest_manager.read().await.has_update().await
    }

    /// Installs the manifest changes from the current version to the target version (inclusive).
    ///
    /// Returns installed [RegionManifest].
    /// **Note**: This function is not guaranteed to install the target version strictly.
    /// The installed version may be greater than the target version.
    pub(crate) async fn install_manifest_to(
        &self,
        version: ManifestVersion,
    ) -> Result<Arc<RegionManifest>> {
        let mut manager = self.manifest_manager.write().await;
        manager.install_manifest_to(version).await?;

        Ok(manager.manifest())
    }

    /// Updates the manifest if current state is `expect_state`.
    pub(crate) async fn update_manifest(
        &self,
        expect_state: RegionLeaderState,
        action_list: RegionMetaActionList,
    ) -> Result<ManifestVersion> {
        // Acquires the write lock of the manifest manager.
        let mut manager = self.manifest_manager.write().await;
        // Gets current manifest.
        let manifest = manager.manifest();
        // Checks state inside the lock. This is to ensure that we won't update the manifest
        // after `set_readonly_gracefully()` is called.
        let current_state = self.state.load();

        // If expect_state is not downgrading, the current state must be either `expect_state` or downgrading.
        //
        // A downgrading leader rejects user writes but still allows
        // flushing the memtable and updating the manifest.
        if expect_state != RegionLeaderState::Downgrading {
            if current_state == RegionRoleState::Leader(RegionLeaderState::Downgrading) {
                info!(
                    "Region {} is in downgrading leader state, updating manifest. state is {:?}",
                    manifest.metadata.region_id, expect_state
                );
            }
            ensure!(
                current_state == RegionRoleState::Leader(expect_state)
                    || current_state == RegionRoleState::Leader(RegionLeaderState::Downgrading),
                UpdateManifestSnafu {
                    region_id: manifest.metadata.region_id,
                    state: current_state,
                }
            );
        } else {
            ensure!(
                current_state == RegionRoleState::Leader(expect_state),
                RegionStateSnafu {
                    region_id: manifest.metadata.region_id,
                    state: current_state,
                    expect: RegionRoleState::Leader(expect_state),
                }
            );
        }

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
        let version = manager.update(action_list).await.inspect_err(
            |e| error!(e; "Failed to update manifest, region_id: {}", manifest.metadata.region_id),
        )?;

        if self.state.load() == RegionRoleState::Follower {
            warn!(
                "Region {} becomes follower while updating manifest which may cause inconsistency, manifest version: {version}",
                manifest.metadata.region_id
            );
        }

        Ok(version)
    }

    /// Sets the [`RegionRole`].
    ///
    /// ```
    ///     +------------------------------------------+
    ///     |                      +-----------------+ |
    ///     |                      |                 | |
    /// +---+------+       +-------+-----+        +--v-v---+
    /// | Follower |       | Downgrading |        | Leader |
    /// +---^-^----+       +-----+-^-----+        +--+-+---+
    ///     | |                  | |                 | |
    ///     | +------------------+ +-----------------+ |
    ///     +------------------------------------------+
    ///
    /// Transition:
    /// - Follower -> Leader
    /// - Downgrading Leader -> Leader
    /// - Leader -> Follower
    /// - Downgrading Leader -> Follower
    /// - Leader -> Downgrading Leader
    ///
    /// ```
    pub(crate) fn set_role(&self, next_role: RegionRole, region_id: RegionId) {
        match next_role {
            RegionRole::Follower => {
                match self.state.fetch_update(|state| {
                    if !matches!(state, RegionRoleState::Follower) {
                        Some(RegionRoleState::Follower)
                    } else {
                        None
                    }
                }) {
                    Ok(state) => info!(
                        "Convert region {} to follower, previous role state: {:?}",
                        region_id, state
                    ),
                    Err(state) => {
                        if state != RegionRoleState::Follower {
                            warn!(
                                "Failed to convert region {} to follower, current role state: {:?}",
                                region_id, state
                            )
                        }
                    }
                }
            }
            RegionRole::Leader => {
                match self.state.fetch_update(|state| {
                    if matches!(
                        state,
                        RegionRoleState::Follower
                            | RegionRoleState::Leader(RegionLeaderState::Downgrading)
                    ) {
                        Some(RegionRoleState::Leader(RegionLeaderState::Writable))
                    } else {
                        None
                    }
                }) {
                    Ok(state) => info!(
                        "Convert region {} to leader, previous role state: {:?}",
                        region_id, state
                    ),
                    Err(state) => {
                        if state != RegionRoleState::Leader(RegionLeaderState::Writable) {
                            warn!(
                                "Failed to convert region {} to leader, current role state: {:?}",
                                region_id, state
                            )
                        }
                    }
                }
            }
            RegionRole::DowngradingLeader => {
                match self.state.compare_exchange(
                    RegionRoleState::Leader(RegionLeaderState::Writable),
                    RegionRoleState::Leader(RegionLeaderState::Downgrading),
                ) {
                    Ok(state) => info!(
                        "Convert region {} to downgrading region, previous role state: {:?}",
                        region_id, state
                    ),
                    Err(state) => {
                        if state != RegionRoleState::Leader(RegionLeaderState::Downgrading) {
                            warn!(
                                "Failed to convert region {} to downgrading leader, current role state: {:?}",
                                region_id, state
                            )
                        }
                    }
                }
            }
        }
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
                expect: RegionRoleState::Leader(RegionLeaderState::Writable),
            }
        );
        Ok(region)
    }

    /// Gets readonly region by region id.
    ///
    /// Returns error if the region does not exist or is writable.
    pub(crate) fn follower_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            region.is_follower(),
            RegionStateSnafu {
                region_id,
                state: region.state(),
                expect: RegionRoleState::Follower,
            }
        );

        Ok(region)
    }

    /// Gets region by region id.
    ///
    /// Calls the callback if the region does not exist.
    pub(crate) fn get_region_or<F: OnFailure>(
        &self,
        region_id: RegionId,
        cb: &mut F,
    ) -> Option<MitoRegionRef> {
        match self
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })
        {
            Ok(region) => Some(region),
            Err(e) => {
                cb.on_failure(e);
                None
            }
        }
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

    /// Gets flushable region by region id.
    ///
    /// Returns error if the region does not exist or is not operable.
    fn flushable_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            region.is_flushable(),
            FlushableRegionStateSnafu {
                region_id,
                state: region.state(),
            }
        );
        Ok(region)
    }

    /// Gets flushable region by region id.
    ///
    /// Calls the callback if the region does not exist or is not operable.
    pub(crate) fn flushable_region_or<F: OnFailure>(
        &self,
        region_id: RegionId,
        cb: &mut F,
    ) -> Option<MitoRegionRef> {
        match self.flushable_region(region_id) {
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
    manifest_version: Arc<AtomicU64>,
}

impl ManifestStats {
    fn total_manifest_size(&self) -> u64 {
        self.total_manifest_size.load(Ordering::Relaxed)
    }

    fn manifest_version(&self) -> u64 {
        self.manifest_version.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crossbeam_utils::atomic::AtomicCell;
    use store_api::region_engine::RegionRole;
    use store_api::storage::RegionId;

    use crate::region::{RegionLeaderState, RegionRoleState};
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::VersionControlBuilder;

    #[test]
    fn test_region_state_lock_free() {
        assert!(AtomicCell::<RegionRoleState>::is_lock_free());
    }

    #[tokio::test]
    async fn test_set_region_state() {
        let env = SchedulerEnv::new().await;
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let manifest_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;

        let region_id = RegionId::new(1024, 0);
        // Leader -> Follower
        manifest_ctx.set_role(RegionRole::Follower, region_id);
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Follower -> Leader
        manifest_ctx.set_role(RegionRole::Leader, region_id);
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );

        // Leader -> Downgrading Leader
        manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id);
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        );

        // Downgrading Leader -> Follower
        manifest_ctx.set_role(RegionRole::Follower, region_id);
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Can't downgrade from follower (Follower -> Downgrading Leader)
        manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id);
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Set region role too Downgrading Leader
        manifest_ctx.set_role(RegionRole::Leader, region_id);
        manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id);
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        );

        // Downgrading Leader -> Leader
        manifest_ctx.set_role(RegionRole::Leader, region_id);
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
    }
}
