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

pub mod catchup;
pub mod opener;
pub mod options;
pub(crate) mod version;

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use common_telemetry::{debug, error, info, warn};
use crossbeam_utils::atomic::AtomicCell;
use snafu::{OptionExt, ensure};
use store_api::ManifestVersion;
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::provider::Provider;
use store_api::metadata::RegionMetadataRef;
use store_api::region_engine::{
    RegionManifestInfo, RegionRole, RegionStatistic, SettableRegionRoleState,
};
use store_api::sst_entry::ManifestSstEntry;
use store_api::storage::{RegionId, SequenceNumber};
use tokio::sync::RwLockWriteGuard;

use crate::access_layer::AccessLayerRef;
use crate::error::{
    FlushableRegionStateSnafu, RegionNotFoundSnafu, RegionStateSnafu, RegionTruncatedSnafu, Result,
    UpdateManifestSnafu,
};
use crate::manifest::action::{
    RegionChange, RegionManifest, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::manager::RegionManifestManager;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::{OnFailure, OptionOutputTx};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::location::{index_file_path, sst_file_path};
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
    /// The region is in staging mode - writable but no checkpoint/compaction.
    Staging,
    /// The region is entering staging mode. - write requests will be stalled.
    EnteringStaging,
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
pub struct MitoRegion {
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
    /// The value will be updated to the latest offset of the topic
    /// if region receives a flush request or schedules a periodic flush task
    /// and the region's memtable is empty.
    ///
    /// There are no WAL entries in range [flushed_entry_id, topic_latest_entry_id] for current region,
    /// which means these WAL entries maybe able to be pruned up to `topic_latest_entry_id`.
    pub(crate) topic_latest_entry_id: AtomicU64,
    /// The total bytes written to the region.
    pub(crate) written_bytes: Arc<AtomicU64>,
    /// The partition expression of the region in staging mode.
    ///
    /// During the staging mode, the region metadata in [`VersionControlRef`] is not updated,
    /// so we need to store the partition expression separately.
    /// TODO(weny):
    /// 1. Reload the staging partition expr during region open.
    /// 2. Rejects requests with mismatching partition expr.
    pub(crate) staging_partition_expr: Mutex<Option<String>>,
    /// manifest stats
    stats: ManifestStats,
}

pub type MitoRegionRef = Arc<MitoRegion>;

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

    /// Returns last compaction timestamp in millis.
    pub(crate) fn last_compaction_millis(&self) -> i64 {
        self.last_compaction_millis.load(Ordering::Relaxed)
    }

    /// Update compaction time to current time.
    pub(crate) fn update_compaction_millis(&self) {
        let now = self.time_provider.current_time_millis();
        self.last_compaction_millis.store(now, Ordering::Relaxed);
    }

    /// Returns the table dir.
    pub(crate) fn table_dir(&self) -> &str {
        self.access_layer.table_dir()
    }

    /// Returns whether the region is writable.
    pub(crate) fn is_writable(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
                | RegionRoleState::Leader(RegionLeaderState::Staging)
        )
    }

    /// Returns whether the region is flushable.
    pub(crate) fn is_flushable(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
                | RegionRoleState::Leader(RegionLeaderState::Staging)
                | RegionRoleState::Leader(RegionLeaderState::Downgrading)
        )
    }

    /// Returns whether the region should abort index building.
    pub(crate) fn should_abort_index(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Follower
                | RegionRoleState::Leader(RegionLeaderState::Dropping)
                | RegionRoleState::Leader(RegionLeaderState::Truncating)
                | RegionRoleState::Leader(RegionLeaderState::Downgrading)
                | RegionRoleState::Leader(RegionLeaderState::Staging)
        )
    }

    /// Returns whether the region is downgrading.
    pub(crate) fn is_downgrading(&self) -> bool {
        matches!(
            self.manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        )
    }

    /// Returns whether the region is in staging mode.
    #[allow(dead_code)]
    pub(crate) fn is_staging(&self) -> bool {
        self.manifest_ctx.state.load() == RegionRoleState::Leader(RegionLeaderState::Staging)
    }

    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn find_committed_sequence(&self) -> SequenceNumber {
        self.version_control.committed_sequence()
    }

    /// Returns whether the region is readonly.
    pub fn is_follower(&self) -> bool {
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

    /// Sets the staging state.
    ///
    /// You should call this method in the worker loop.
    /// Transitions from Writable to Staging state.
    /// Cleans any existing staging manifests before entering staging mode.
    pub(crate) async fn set_staging(
        &self,
        manager: &mut RwLockWriteGuard<'_, RegionManifestManager>,
    ) -> Result<()> {
        manager.store().clear_staging_manifests().await?;

        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::Staging),
        )
    }

    /// Sets the entering staging state.
    pub(crate) fn set_entering_staging(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Writable,
            RegionRoleState::Leader(RegionLeaderState::EnteringStaging),
        )
    }

    /// Exits the entering staging state back to writable.
    pub(crate) fn exit_entering_staging(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::EnteringStaging,
            RegionRoleState::Leader(RegionLeaderState::Writable),
        )
    }

    /// Exits the staging state back to writable.
    ///
    /// You should call this method in the worker loop.
    /// Transitions from Staging to Writable state.
    pub fn exit_staging(&self) -> Result<()> {
        self.compare_exchange_state(
            RegionLeaderState::Staging,
            RegionRoleState::Leader(RegionLeaderState::Writable),
        )
    }

    /// Sets the region role state gracefully. This acquires the manifest write lock.
    pub(crate) async fn set_role_state_gracefully(
        &self,
        state: SettableRegionRoleState,
    ) -> Result<()> {
        let mut manager = self.manifest_ctx.manifest_manager.write().await;
        let current_state = self.state();

        match state {
            SettableRegionRoleState::Leader => {
                // Exit staging mode and return to normal writable leader
                // Only allowed from staging state
                match current_state {
                    RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        info!("Exiting staging mode for region {}", self.region_id);
                        // Use the success exit path that merges all staged manifests
                        self.exit_staging_on_success(&mut manager).await?;
                    }
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        // Already in desired state - no-op
                        info!("Region {} already in normal leader mode", self.region_id);
                    }
                    _ => {
                        // Only staging -> leader transition is allowed
                        return Err(RegionStateSnafu {
                            region_id: self.region_id,
                            state: current_state,
                            expect: RegionRoleState::Leader(RegionLeaderState::Staging),
                        }
                        .build());
                    }
                }
            }

            SettableRegionRoleState::StagingLeader => {
                // Enter staging mode from normal writable leader
                // Only allowed from writable leader state
                match current_state {
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        info!("Entering staging mode for region {}", self.region_id);
                        self.set_staging(&mut manager).await?;
                    }
                    RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        // Already in desired state - no-op
                        info!("Region {} already in staging mode", self.region_id);
                    }
                    _ => {
                        return Err(RegionStateSnafu {
                            region_id: self.region_id,
                            state: current_state,
                            expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                        }
                        .build());
                    }
                }
            }

            SettableRegionRoleState::Follower => {
                // Make this region a follower
                match current_state {
                    RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        info!(
                            "Exiting staging and demoting region {} to follower",
                            self.region_id
                        );
                        self.exit_staging()?;
                        self.set_role(RegionRole::Follower);
                    }
                    RegionRoleState::Leader(_) => {
                        info!("Demoting region {} from leader to follower", self.region_id);
                        self.set_role(RegionRole::Follower);
                    }
                    RegionRoleState::Follower => {
                        // Already in desired state - no-op
                        info!("Region {} already in follower mode", self.region_id);
                    }
                }
            }

            SettableRegionRoleState::DowngradingLeader => {
                // downgrade this region to downgrading leader
                match current_state {
                    RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        info!(
                            "Exiting staging and entering downgrade for region {}",
                            self.region_id
                        );
                        self.exit_staging()?;
                        self.set_role(RegionRole::DowngradingLeader);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        info!("Starting downgrade for region {}", self.region_id);
                        self.set_role(RegionRole::DowngradingLeader);
                    }
                    RegionRoleState::Leader(RegionLeaderState::Downgrading) => {
                        // Already in desired state - no-op
                        info!("Region {} already in downgrading mode", self.region_id);
                    }
                    _ => {
                        warn!(
                            "Cannot start downgrade for region {} from state {:?}",
                            self.region_id, current_state
                        );
                    }
                }
            }
        }

        // Hack(zhongzc): If we have just become leader (writable), persist any backfilled metadata.
        if self.state() == RegionRoleState::Leader(RegionLeaderState::Writable) {
            // Persist backfilled metadata if manifest is missing fields (e.g., partition_expr)
            let manifest_meta = &manager.manifest().metadata;
            let current_version = self.version();
            let current_meta = &current_version.metadata;
            if manifest_meta.partition_expr.is_none() && current_meta.partition_expr.is_some() {
                let action = RegionMetaAction::Change(RegionChange {
                    metadata: current_meta.clone(),
                    sst_format: current_version.options.sst_format.unwrap_or_default(),
                });
                let result = manager
                    .update(RegionMetaActionList::with_action(action), false)
                    .await;

                match result {
                    Ok(version) => {
                        info!(
                            "Successfully persisted backfilled metadata for region {}, version: {}",
                            self.region_id, version
                        );
                    }
                    Err(e) => {
                        warn!(e; "Failed to persist backfilled metadata for region {}", self.region_id);
                    }
                }
            }
        }

        drop(manager);

        Ok(())
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

    /// Switches the region state to `RegionRoleState::Leader(RegionLeaderState::Staging)` if the current state is `expect`.
    /// Otherwise, logs an error.
    pub(crate) fn switch_state_to_staging(&self, expect: RegionLeaderState) {
        if let Err(e) =
            self.compare_exchange_state(expect, RegionRoleState::Leader(RegionLeaderState::Staging))
        {
            error!(e; "failed to switch region state to staging, expect state is {:?}", expect);
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
        let num_files = version.ssts.num_files();
        let manifest_version = self.stats.manifest_version();
        let file_removed_cnt = self.stats.file_removed_cnt();

        let topic_latest_entry_id = self.topic_latest_entry_id.load(Ordering::Relaxed);
        let written_bytes = self.written_bytes.load(Ordering::Relaxed);

        RegionStatistic {
            num_rows,
            memtable_size: memtable_usage,
            wal_size: wal_usage,
            manifest_size: manifest_usage,
            sst_size: sst_usage,
            sst_num: num_files,
            index_size: index_usage,
            manifest: RegionManifestInfo::Mito {
                manifest_version,
                flushed_entry_id,
                file_removed_cnt,
            },
            data_topic_latest_entry_id: topic_latest_entry_id,
            metadata_topic_latest_entry_id: topic_latest_entry_id,
            written_bytes,
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

    pub fn access_layer(&self) -> AccessLayerRef {
        self.access_layer.clone()
    }

    /// Returns the SST entries of the region.
    pub async fn manifest_sst_entries(&self) -> Vec<ManifestSstEntry> {
        let table_dir = self.table_dir();
        let path_type = self.access_layer.path_type();

        let visible_ssts = self
            .version()
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files().map(|file| file.file_id().file_id()))
            .collect::<HashSet<_>>();

        let manifest_files = self.manifest_ctx.manifest().await.files.clone();
        let staging_files = self
            .manifest_ctx
            .staging_manifest()
            .await
            .map(|m| m.files.clone())
            .unwrap_or_default();
        let files = manifest_files
            .into_iter()
            .chain(staging_files.into_iter())
            .collect::<HashMap<_, _>>();

        files
            .values()
            .map(|meta| {
                let region_id = self.region_id;
                let origin_region_id = meta.region_id;
                let (index_file_id, index_file_path, index_file_size) = if meta.index_file_size > 0
                {
                    let index_file_path =
                        index_file_path(table_dir, meta.index_file_id(), path_type);
                    (
                        Some(meta.index_file_id().file_id().to_string()),
                        Some(index_file_path),
                        Some(meta.index_file_size),
                    )
                } else {
                    (None, None, None)
                };
                let visible = visible_ssts.contains(&meta.file_id);
                ManifestSstEntry {
                    table_dir: table_dir.to_string(),
                    region_id,
                    table_id: region_id.table_id(),
                    region_number: region_id.region_number(),
                    region_group: region_id.region_group(),
                    region_sequence: region_id.region_sequence(),
                    file_id: meta.file_id.to_string(),
                    index_file_id,
                    level: meta.level,
                    file_path: sst_file_path(table_dir, meta.file_id(), path_type),
                    file_size: meta.file_size,
                    index_file_path,
                    index_file_size,
                    num_rows: meta.num_rows,
                    num_row_groups: meta.num_row_groups,
                    num_series: Some(meta.num_series),
                    min_ts: meta.time_range.0,
                    max_ts: meta.time_range.1,
                    sequence: meta.sequence.map(|s| s.get()),
                    origin_region_id,
                    node_id: None,
                    visible,
                }
            })
            .collect()
    }

    /// Exit staging mode successfully by merging all staged manifests and making them visible.
    pub(crate) async fn exit_staging_on_success(
        &self,
        manager: &mut RwLockWriteGuard<'_, RegionManifestManager>,
    ) -> Result<()> {
        let current_state = self.manifest_ctx.current_state();
        ensure!(
            current_state == RegionRoleState::Leader(RegionLeaderState::Staging),
            RegionStateSnafu {
                region_id: self.region_id,
                state: current_state,
                expect: RegionRoleState::Leader(RegionLeaderState::Staging),
            }
        );

        // Merge all staged manifest actions
        let merged_actions = match manager.merge_staged_actions(current_state).await? {
            Some(actions) => actions,
            None => {
                info!(
                    "No staged manifests to merge for region {}, exiting staging mode without changes",
                    self.region_id
                );
                // Even if no manifests to merge, we still need to exit staging mode
                self.exit_staging()?;
                return Ok(());
            }
        };

        // Submit merged actions using the manifest manager's update method
        // Pass the `false` so it saves to normal directory, not staging
        let new_version = manager.update(merged_actions.clone(), false).await?;

        info!(
            "Successfully submitted merged staged manifests for region {}, new version: {}",
            self.region_id, new_version
        );

        // Apply the merged changes to in-memory version control
        let merged_edit = merged_actions.into_region_edit();
        self.version_control
            .apply_edit(Some(merged_edit), &[], self.file_purger.clone());

        // Clear all staging manifests and transit state
        manager.store().clear_staging_manifests().await?;
        self.exit_staging()?;

        Ok(())
    }
}

/// Context to update the region manifest.
#[derive(Debug)]
pub(crate) struct ManifestContext {
    /// Manager to maintain manifest for this region.
    pub(crate) manifest_manager: tokio::sync::RwLock<RegionManifestManager>,
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

    /// Returns the current region role state.
    pub(crate) fn current_state(&self) -> RegionRoleState {
        self.state.load()
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
        is_staging: bool,
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
        let version = manager.update(action_list, is_staging).await.inspect_err(
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
    /// ```text
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

    /// Returns the normal manifest of the region.
    pub(crate) async fn manifest(&self) -> Arc<crate::manifest::action::RegionManifest> {
        self.manifest_manager.read().await.manifest()
    }

    /// Returns the staging manifest of the region.
    pub(crate) async fn staging_manifest(
        &self,
    ) -> Option<Arc<crate::manifest::action::RegionManifest>> {
        self.manifest_manager.read().await.staging_manifest()
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

    /// Gets writable non-staging region by region id.
    ///
    /// Returns error if the region does not exist, is readonly, or is in staging mode.
    pub(crate) fn writable_non_staging_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self.writable_region(region_id)?;
        if region.is_staging() {
            return Err(crate::error::RegionStateSnafu {
                region_id,
                state: region.state(),
                expect: RegionRoleState::Leader(RegionLeaderState::Writable),
            }
            .build());
        }
        Ok(region)
    }

    /// Gets staging region by region id.
    ///
    /// Returns error if the region does not exist or is not in staging state.
    pub(crate) fn staging_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
        let region = self
            .get_region(region_id)
            .context(RegionNotFoundSnafu { region_id })?;
        ensure!(
            region.is_staging(),
            RegionStateSnafu {
                region_id,
                state: region.state(),
                expect: RegionRoleState::Leader(RegionLeaderState::Staging),
            }
        );
        Ok(region)
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

/// The regions that are catching up.
#[derive(Debug, Default)]
pub(crate) struct CatchupRegions {
    regions: RwLock<HashSet<RegionId>>,
}

impl CatchupRegions {
    /// Returns true if the region exists.
    pub(crate) fn is_region_exists(&self, region_id: RegionId) -> bool {
        let regions = self.regions.read().unwrap();
        regions.contains(&region_id)
    }

    /// Inserts a new region into the set.
    pub(crate) fn insert_region(&self, region_id: RegionId) {
        let mut regions = self.regions.write().unwrap();
        regions.insert(region_id);
    }

    /// Remove region by id.
    pub(crate) fn remove_region(&self, region_id: RegionId) {
        let mut regions = self.regions.write().unwrap();
        regions.remove(&region_id);
    }
}

pub(crate) type CatchupRegionsRef = Arc<CatchupRegions>;

/// Manifest stats.
#[derive(Default, Debug, Clone)]
pub struct ManifestStats {
    pub(crate) total_manifest_size: Arc<AtomicU64>,
    pub(crate) manifest_version: Arc<AtomicU64>,
    pub(crate) file_removed_cnt: Arc<AtomicU64>,
}

impl ManifestStats {
    fn total_manifest_size(&self) -> u64 {
        self.total_manifest_size.load(Ordering::Relaxed)
    }

    fn manifest_version(&self) -> u64 {
        self.manifest_version.load(Ordering::Relaxed)
    }

    fn file_removed_cnt(&self) -> u64 {
        self.file_removed_cnt.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;
    use std::sync::{Arc, Mutex};

    use common_datasource::compression::CompressionType;
    use common_test_util::temp_dir::create_temp_dir;
    use crossbeam_utils::atomic::AtomicCell;
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use store_api::logstore::provider::Provider;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::PathType;
    use store_api::storage::RegionId;

    use crate::access_layer::AccessLayer;
    use crate::manifest::action::RegionMetaActionList;
    use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
    use crate::region::{
        ManifestContext, ManifestStats, MitoRegion, RegionLeaderState, RegionRoleState,
    };
    use crate::sst::FormatType;
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::VersionControlBuilder;
    use crate::time_provider::StdTimeProvider;

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

    #[tokio::test]
    async fn test_staging_state_validation() {
        let env = SchedulerEnv::new().await;
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());

        // Create context with staging state using the correct pattern from SchedulerEnv
        let staging_ctx = {
            let manager = RegionManifestManager::new(
                version_control.current().version.metadata.clone(),
                0,
                RegionManifestOptions {
                    manifest_dir: "".to_string(),
                    object_store: env.access_layer.object_store().clone(),
                    compress_type: CompressionType::Uncompressed,
                    checkpoint_distance: 10,
                    remove_file_options: Default::default(),
                },
                FormatType::PrimaryKey,
                &Default::default(),
            )
            .await
            .unwrap();
            Arc::new(ManifestContext::new(
                manager,
                RegionRoleState::Leader(RegionLeaderState::Staging),
            ))
        };

        // Test staging state behavior
        assert_eq!(
            staging_ctx.current_state(),
            RegionRoleState::Leader(RegionLeaderState::Staging)
        );

        // Test writable context for comparison
        let writable_ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;

        assert_eq!(
            writable_ctx.current_state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
    }

    #[tokio::test]
    async fn test_staging_state_transitions() {
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let metadata = version_control.current().version.metadata.clone();

        // Create MitoRegion for testing state transitions
        let temp_dir = create_temp_dir("");
        let path_str = temp_dir.path().display().to_string();
        let fs_builder = Fs::default().root(&path_str);
        let object_store = ObjectStore::new(fs_builder).unwrap().finish();

        let index_aux_path = temp_dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let access_layer = Arc::new(AccessLayer::new(
            "",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let manager = RegionManifestManager::new(
            metadata.clone(),
            0,
            RegionManifestOptions {
                manifest_dir: "".to_string(),
                object_store: access_layer.object_store().clone(),
                compress_type: CompressionType::Uncompressed,
                checkpoint_distance: 10,
                remove_file_options: Default::default(),
            },
            FormatType::PrimaryKey,
            &Default::default(),
        )
        .await
        .unwrap();

        let manifest_ctx = Arc::new(ManifestContext::new(
            manager,
            RegionRoleState::Leader(RegionLeaderState::Writable),
        ));

        let region = MitoRegion {
            region_id: metadata.region_id,
            version_control,
            access_layer,
            manifest_ctx: manifest_ctx.clone(),
            file_purger: crate::test_util::new_noop_file_purger(),
            provider: Provider::noop_provider(),
            last_flush_millis: Default::default(),
            last_compaction_millis: Default::default(),
            time_provider: Arc::new(StdTimeProvider),
            topic_latest_entry_id: Default::default(),
            written_bytes: Arc::new(AtomicU64::new(0)),
            stats: ManifestStats::default(),
            staging_partition_expr: Mutex::new(None),
        };

        // Test initial state
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
        assert!(!region.is_staging());

        // Test transition to staging
        let mut manager = manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();
        drop(manager);
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Staging)
        );
        assert!(region.is_staging());

        // Test transition back to writable
        region.exit_staging().unwrap();
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
        assert!(!region.is_staging());

        // Test staging directory cleanup: Create dirty staging files before entering staging mode
        {
            // Create some dummy staging manifest files to simulate interrupted session
            let manager = manifest_ctx.manifest_manager.write().await;
            let dummy_actions = RegionMetaActionList::new(vec![]);
            let dummy_bytes = dummy_actions.encode().unwrap();

            // Create dirty staging files with versions 100 and 101
            manager.store().save(100, &dummy_bytes, true).await.unwrap();
            manager.store().save(101, &dummy_bytes, true).await.unwrap();
            drop(manager);

            // Verify dirty files exist before entering staging
            let manager = manifest_ctx.manifest_manager.read().await;
            let dirty_manifests = manager.store().fetch_staging_manifests().await.unwrap();
            assert_eq!(
                dirty_manifests.len(),
                2,
                "Should have 2 dirty staging files"
            );
            drop(manager);

            // Enter staging mode - this should clean up the dirty files
            let mut manager = manifest_ctx.manifest_manager.write().await;
            region.set_staging(&mut manager).await.unwrap();
            drop(manager);

            // Verify dirty files are cleaned up after entering staging
            let manager = manifest_ctx.manifest_manager.read().await;
            let cleaned_manifests = manager.store().fetch_staging_manifests().await.unwrap();
            assert_eq!(
                cleaned_manifests.len(),
                0,
                "Dirty staging files should be cleaned up"
            );
            drop(manager);

            // Exit staging to restore normal state for remaining tests
            region.exit_staging().unwrap();
        }

        // Test invalid transitions
        let mut manager = manifest_ctx.manifest_manager.write().await;
        assert!(region.set_staging(&mut manager).await.is_ok()); // Writable -> Staging should work
        drop(manager);
        let mut manager = manifest_ctx.manifest_manager.write().await;
        assert!(region.set_staging(&mut manager).await.is_err()); // Staging -> Staging should fail
        drop(manager);
        assert!(region.exit_staging().is_ok()); // Staging -> Writable should work
        assert!(region.exit_staging().is_err()); // Writable -> Writable should fail
    }
}
