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
pub mod utils;
pub(crate) mod version;

use std::cell::OnceCell;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use common_base::hash::partition_expr_version;
use common_recordbatch::adapter::RegionQueryStatCounters;
use common_recordbatch::filter::MetadataFilter;
use common_telemetry::{error, info, warn};
use common_time::timestamp::TimeUnit;
use crossbeam_utils::atomic::AtomicCell;
use datafusion_common::ScalarValue;
use futures::stream::{BoxStream, StreamExt};
use partition::expr::PartitionExpr;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::ManifestVersion;
use store_api::codec::PrimaryKeyEncoding;
use store_api::logstore::provider::Provider;
use store_api::metadata::RegionMetadataRef;
use store_api::metrics::{REGION_QUERY_CPU_TIME, REGION_QUERY_SCANNED_BYTES};
use store_api::region_engine::{
    RegionManifestInfo, RegionRole, RegionStatistic, SettableRegionRoleState,
};
use store_api::region_info::RegionInfoEntry;
use store_api::region_request::{PathType, StagingPartitionDirective};
use store_api::sst_entry::{self, ManifestSstEntry};
use store_api::storage::{FileId, RegionId, SequenceNumber};
use tokio::sync::RwLockWriteGuard;
pub use utils::*;

use crate::access_layer::AccessLayerRef;
use crate::compaction::CompactionStateRef;
use crate::engine::region_hook::{PendingManifestHook, RegionHookRef};
use crate::error::{
    EndStagingSnafu, Error, FlushableRegionStateSnafu, InvalidPartitionExprSnafu,
    InvalidRequestSnafu, RegionBusySnafu, RegionLeaseExpiredSnafu, RegionLeaseNotReceivedSnafu,
    RegionNotFoundSnafu, RegionStateSnafu, RegionStoppedSnafu, RegionTruncatedSnafu, Result,
    UnexpectedSnafu, UpdateManifestSnafu,
};
use crate::lease::{Deadline, NodeLease, lease_nanos};
use crate::manifest::action::{
    PendingTail, RegionChange, RegionEdit, RegionManifest, RegionMetaAction, RegionMetaActionList,
};
use crate::manifest::manager::RegionManifestManager;
use crate::metrics::manifest_size::RegionManifestSizeMetric;
use crate::region::version::{VersionControlRef, VersionRef};
use crate::request::{OnFailure, OptionOutputTx};
use crate::sst::file::{self, FileMeta};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::location::index_file_path;
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
    /// The region is in staging mode - writable but no checkpoint/compaction/flush.
    Staging,
    /// The region is entering staging mode. - write requests will be stalled.
    EnteringStaging,
    /// The region is committing its staged edits in `end_staging`. Writes stall until
    /// `StagingEnded` has the worker handle them, after the region leaves this state. `set_role`
    /// ignores role changes, since `leave_ending_staging` works out the next state from the
    /// commit result, a downgrade begun meanwhile and the Leader lease. Graceful role changes
    /// wait for the manifest lock, which `end_staging` holds until the region leaves.
    EndingStaging,
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

impl RegionRoleState {
    /// Returns whether a region in this state can replay pending tails: a leader that is
    /// Writable, Altering or Editing. A region with pending tails cannot enter staging; a
    /// downgrading one is leaving.
    pub(crate) fn can_replay_tails(self) -> bool {
        matches!(
            self,
            RegionRoleState::Leader(
                RegionLeaderState::Writable
                    | RegionLeaderState::Altering
                    | RegionLeaderState::Editing
            )
        )
    }

    /// Converts the region role state to leader state if it is a leader state.
    pub fn into_leader_state(self) -> Option<RegionLeaderState> {
        match self {
            RegionRoleState::Leader(leader_state) => Some(leader_state),
            RegionRoleState::Follower => None,
        }
    }

    /// Returns whether a region in this state is admitted to flush.
    pub(crate) fn is_flushable(self) -> bool {
        matches!(
            self,
            Self::Leader(RegionLeaderState::Writable)
                | Self::Leader(RegionLeaderState::Staging)
                | Self::Leader(RegionLeaderState::Downgrading)
        )
    }

    /// Returns whether the region is staging or ending staging. A staged edit belongs to the
    /// session too, which only [MitoRegion::in_staging_session] can tell.
    pub(crate) fn in_staging_session(self) -> bool {
        self == Self::Leader(RegionLeaderState::Staging)
            || self == Self::Leader(RegionLeaderState::EndingStaging)
    }

    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            RegionRoleState::Follower => "Follower",
            RegionRoleState::Leader(RegionLeaderState::Writable) => "Leader(Writable)",
            RegionRoleState::Leader(RegionLeaderState::Staging) => "Leader(Staging)",
            RegionRoleState::Leader(RegionLeaderState::EnteringStaging) => {
                "Leader(EnteringStaging)"
            }
            RegionRoleState::Leader(RegionLeaderState::EndingStaging) => "Leader(EndingStaging)",
            RegionRoleState::Leader(RegionLeaderState::Altering) => "Leader(Altering)",
            RegionRoleState::Leader(RegionLeaderState::Dropping) => "Leader(Dropping)",
            RegionRoleState::Leader(RegionLeaderState::Truncating) => "Leader(Truncating)",
            RegionRoleState::Leader(RegionLeaderState::Editing) => "Leader(Editing)",
            RegionRoleState::Leader(RegionLeaderState::Downgrading) => "Leader(Downgrading)",
        }
    }
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
    /// Live compaction state shared with readers without routing through the worker.
    pub(crate) compaction_state: CompactionStateRef,

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
    /// Last schedule compaction time in millis.
    last_schedule_compaction_millis: AtomicI64,
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
    /// Region stats.
    pub(crate) region_stats: RegionStats,
    /// manifest stats
    stats: ManifestStats,
}

/// Runtime statistics of a region.
#[derive(Debug)]
pub(crate) struct RegionStats {
    /// The total bytes written to the region.
    pub(crate) written_bytes: Arc<AtomicU64>,
    /// Parquet bytes committed by flushes since this region instance was opened.
    pub(crate) ingested_parquet_bytes: AtomicU64,
    /// The total query CPU time of the region in nanoseconds.
    pub(crate) query_cpu_time: Arc<AtomicU64>,
    /// The total scanned bytes of the region.
    pub(crate) query_scanned_bytes: Arc<AtomicU64>,
}

impl RegionStats {
    pub(crate) fn new() -> Self {
        Self {
            written_bytes: Arc::new(AtomicU64::new(0)),
            ingested_parquet_bytes: AtomicU64::new(0),
            query_cpu_time: Arc::new(AtomicU64::new(0)),
            query_scanned_bytes: Arc::new(AtomicU64::new(0)),
        }
    }

    pub(crate) fn query_stat_counters(&self) -> RegionQueryStatCounters {
        RegionQueryStatCounters {
            query_cpu_time: self.query_cpu_time.clone(),
            query_scanned_bytes: self.query_scanned_bytes.clone(),
        }
    }
}

pub type MitoRegionRef = Arc<MitoRegion>;

#[derive(Debug, Clone)]
pub(crate) struct StagingPartitionInfo {
    pub(crate) partition_directive: StagingPartitionDirective,
    pub(crate) partition_rule_version: u64,
}

impl StagingPartitionInfo {
    /// Returns the partition expression carried by the staging directive, if any.
    pub(crate) fn partition_expr(&self) -> Option<&str> {
        self.partition_directive.partition_expr()
    }

    /// Builds staging partition info from a directive and derives its version marker.
    pub(crate) fn from_partition_directive(partition_directive: StagingPartitionDirective) -> Self {
        let partition_rule_version = match &partition_directive {
            StagingPartitionDirective::UpdatePartitionExpr(expr) => {
                partition_expr_version(Some(expr))
            }
            StagingPartitionDirective::RejectAllWrites => 0,
        };
        Self {
            partition_directive,
            partition_rule_version,
        }
    }
}

impl MitoRegion {
    /// Publishes the current size when this instance enters the live map.
    pub(crate) fn activate_region_manifest_size_metric(&self) {
        if let Some(metric) = self.stats.metric() {
            metric.activate(&self.stats.total_manifest_size);
        }
    }

    /// Injects a metric threshold for lifecycle tests without changing process env.
    #[cfg(test)]
    pub(crate) fn set_manifest_size_metric_threshold(&self, threshold: u64) {
        let metric = self.stats.metric().unwrap();
        metric.set_test_threshold(threshold);
        metric.publish(self.stats.total_manifest_size());
    }

    /// Revokes publication before removal or replacement of this instance.
    pub(crate) fn deactivate_region_manifest_size_metric(&self) {
        if let Some(metric) = self.stats.metric() {
            metric.deactivate();
        }
    }

    fn remove_region_metrics(&self) {
        let region_id = self.region_id.as_u64().to_string();
        let labels = &[region_id.as_str()];
        let _ = REGION_QUERY_CPU_TIME.remove_label_values(labels);
        let _ = REGION_QUERY_SCANNED_BYTES.remove_label_values(labels);
    }

    /// Stops background managers for this region and fences off further promotions.
    pub(crate) async fn stop(&self) {
        self.manifest_ctx.promotion_fence.lock().unwrap().stopped = true;
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
    pub fn metadata(&self) -> RegionMetadataRef {
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

    /// Returns whether writes to this region should skip WAL.
    pub(crate) fn skip_wal(&self) -> bool {
        self.provider == Provider::Noop || self.version().options.skip_wal
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

    /// Returns last schedule compaction timestamp in millis.
    pub(crate) fn last_schedule_compaction_millis(&self) -> i64 {
        self.last_schedule_compaction_millis.load(Ordering::Relaxed)
    }

    /// Update schedule compaction time to current time.
    pub(crate) fn update_schedule_compaction_millis(&self) {
        let now = self.time_provider.current_time_millis();
        self.last_schedule_compaction_millis
            .store(now, Ordering::Relaxed);
    }

    /// Returns the table dir.
    pub(crate) fn table_dir(&self) -> &str {
        self.access_layer.table_dir()
    }

    /// Returns the path type of the region.
    pub(crate) fn path_type(&self) -> PathType {
        self.access_layer.path_type()
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
        self.manifest_ctx.state.load().is_flushable()
    }

    /// Returns whether the region should abort index building.
    pub(crate) fn should_abort_index(&self) -> bool {
        let state = self.manifest_ctx.state.load();
        state.in_staging_session()
            || matches!(
                state,
                RegionRoleState::Follower
                    | RegionRoleState::Leader(RegionLeaderState::Dropping)
                    | RegionRoleState::Leader(RegionLeaderState::Truncating)
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

    /// Returns whether the region is in staging mode.
    pub(crate) fn is_staging(&self) -> bool {
        self.manifest_ctx.state.load() == RegionRoleState::Leader(RegionLeaderState::Staging)
    }

    /// Returns whether background flush triggers must skip the region: a staging region keeps
    /// its rows in the WAL until staging ends, and a region that awaits its Leader lease cannot
    /// commit a flush yet.
    pub(crate) fn defers_background_flush(&self) -> bool {
        self.is_staging() || self.manifest_ctx.awaits_leader_lease()
    }

    /// Returns whether the region is in a staging session: staging, ending staging, or in a
    /// staged edit.
    pub(crate) fn in_staging_session(&self) -> bool {
        match self.manifest_ctx.state.load() {
            state if state.in_staging_session() => true,
            RegionRoleState::Leader(RegionLeaderState::Editing) => {
                self.manifest_ctx.staging_partition_info().is_some()
            }
            _ => false,
        }
    }

    /// Returns whether the region is entering staging mode.
    pub(crate) fn is_enter_staging(&self) -> bool {
        self.manifest_ctx.state.load()
            == RegionRoleState::Leader(RegionLeaderState::EnteringStaging)
    }

    /// Returns whether the region is truncating.
    pub(crate) fn is_truncating(&self) -> bool {
        self.manifest_ctx.state.load() == RegionRoleState::Leader(RegionLeaderState::Truncating)
    }

    /// Returns whether the region can replay pending tails (see
    /// [RegionRoleState::can_replay_tails]).
    pub(crate) fn can_replay_tails(&self) -> bool {
        self.manifest_ctx.state.load().can_replay_tails()
    }

    pub fn region_id(&self) -> RegionId {
        self.region_id
    }

    pub fn find_committed_sequence(&self) -> SequenceNumber {
        self.version_control.committed_sequence()
    }

    /// Returns the latest sequence that has already been persisted into SSTs.
    ///
    /// Incremental memtable-only reads must use a cursor greater than or equal to
    /// this boundary; older cursors are stale because the corresponding updates may
    /// already have been flushed out of memtables.
    pub fn flushed_sequence(&self) -> SequenceNumber {
        self.version_control.current().version.flushed_sequence
    }

    /// Returns whether the region is readonly.
    pub fn is_follower(&self) -> bool {
        self.manifest_ctx.state.load() == RegionRoleState::Follower
    }

    /// Returns the state of the region.
    pub(crate) fn state(&self) -> RegionRoleState {
        self.manifest_ctx.state.load()
    }

    /// Sets the region role state. Returns the state this call replaced, None if it left the
    /// state unchanged.
    pub(crate) fn set_role(&self, next_role: RegionRole) -> Option<RegionRoleState> {
        self.manifest_ctx.set_role(next_role, self.region_id)
    }

    pub(crate) fn region_role(&self) -> RegionRole {
        match self.state() {
            RegionRoleState::Follower => RegionRole::Follower,
            state if state.in_staging_session() => RegionRole::StagingLeader,
            RegionRoleState::Leader(RegionLeaderState::Downgrading) => {
                RegionRole::DowngradingLeader
            }
            RegionRoleState::Leader(_) => RegionRole::Leader,
        }
    }

    /// Returns whether the region is a leader that no longer awaits its Leader lease, see
    /// [ManifestContext::is_confirmed_leader].
    pub(crate) fn is_confirmed_leader(&self) -> bool {
        self.manifest_ctx.is_confirmed_leader()
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
    pub(crate) fn set_dropping(&self, expect: RegionLeaderState) -> Result<()> {
        self.compare_exchange_state(expect, RegionRoleState::Leader(RegionLeaderState::Dropping))
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
    pub(crate) fn set_editing(&self, expect: RegionLeaderState) -> Result<()> {
        self.compare_exchange_state(expect, RegionRoleState::Leader(RegionLeaderState::Editing))
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
        manager.clear_staging_manifest_and_dir().await?;

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

    /// Exits the staging state back to writable.
    ///
    /// You should call this method in the worker loop.
    /// Transitions from Staging to Writable state.
    pub fn exit_staging(&self) -> Result<()> {
        self.manifest_ctx.exit_staging(
            self.region_id,
            RegionRoleState::Leader(RegionLeaderState::Writable),
        )
    }

    /// Leaves staging without committing it, abandoning a staged edit in progress.
    ///
    /// You should call this method in the worker loop, which is what restores the state of an
    /// edit, and only when [Self::in_staging_session] holds.
    pub(crate) fn abort_staging(&self) {
        let aborted = self
            .manifest_ctx
            .state
            .fetch_update(|state| {
                matches!(
                    state,
                    RegionRoleState::Leader(
                        RegionLeaderState::Staging | RegionLeaderState::Editing
                    )
                )
                .then_some(RegionRoleState::Leader(RegionLeaderState::Writable))
            })
            .is_ok();
        if aborted {
            self.manifest_ctx.clear_staging_partition_info();
        }
    }

    /// Sets the region role state gracefully. This acquires the manifest write lock, and releases
    /// it while a change to follower or downgrading leader waits for the pending checkpoint.
    ///
    /// Ending staging awaits `drain_stalled_writes` (see [Self::end_staging]); other changes
    /// drop it.
    pub(crate) async fn set_role_state_gracefully(
        &self,
        state: SettableRegionRoleState,
        drain_stalled_writes: impl Future<Output = ()>,
    ) -> Result<()> {
        let mut manager: RwLockWriteGuard<'_, RegionManifestManager> =
            self.manifest_ctx.manifest_manager.write().await;
        // Another writer advanced the manifest of a region waiting for a reload, so a migration
        // from its last entry id could skip the rows still in its memtable.
        if matches!(
            state,
            SettableRegionRoleState::Follower | SettableRegionRoleState::DowngradingLeader
        ) && self.manifest_ctx.is_reload_pending()
        {
            return RegionBusySnafu {
                region_id: self.region_id,
            }
            .fail();
        }
        let current_state = self.state();
        let mut wait_for_checkpoint = false;

        let hook_payload: Option<PendingManifestHook> = match state {
            SettableRegionRoleState::Leader => {
                // Exit staging mode and return to normal writable leader
                // Only allowed from staging state
                match current_state {
                    RegionRoleState::Leader(RegionLeaderState::Staging) => {
                        info!("Exiting staging mode for region {}", self.region_id);
                        let pending = self.end_staging(manager, drain_stalled_writes).await?;
                        manager = self.manifest_ctx.manifest_manager.write().await;
                        pending
                    }
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        // Already in desired state - no-op
                        info!("Region {} already in normal leader mode", self.region_id);
                        None
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
                        self.version().ensure_no_pending_tails()?;
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
                None
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
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Leader(_) => {
                        info!("Demoting region {} from leader to follower", self.region_id);
                        self.set_role(RegionRole::Follower);
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Follower => {
                        // Already in desired state. The change that made it a follower may
                        // still wait for the checkpoint.
                        info!("Region {} already in follower mode", self.region_id);
                        wait_for_checkpoint = true;
                    }
                }
                None
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
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Leader(RegionLeaderState::Writable) => {
                        info!("Starting downgrade for region {}", self.region_id);
                        self.set_role(RegionRole::DowngradingLeader);
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Leader(RegionLeaderState::Downgrading) => {
                        // Already in desired state - no-op
                        info!("Region {} already in downgrading mode", self.region_id);
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Follower => {
                        warn!(
                            "Cannot start downgrade for region {} from state {:?}",
                            self.region_id, current_state
                        );
                        wait_for_checkpoint = true;
                    }
                    RegionRoleState::Leader(_) => {
                        // The region may become writable again when the
                        // current operation, such as an ALTER, finishes. Fail
                        // with a retryable error instead of reporting a
                        // downgrade that does not fence writes.
                        return Err(RegionStateSnafu {
                            region_id: self.region_id,
                            state: current_state,
                            expect: RegionRoleState::Leader(RegionLeaderState::Writable),
                        }
                        .build());
                    }
                }
                None
            }
        };

        // The checkpoint task is taken under the lock after the state change, so every commit
        // that could start a checkpoint has registered it: a follower's commit starts none, and a
        // commit of a downgrading leader starts no checkpoint. The wait holds no lock, so the
        // worker and other users of the manifest are not blocked while the checkpoint finishes.
        if wait_for_checkpoint {
            let checkpoint_done = manager.pending_checkpoint_completion();
            drop(manager);
            checkpoint_done.await;
            manager = self.manifest_ctx.manifest_manager.write().await;
        }

        // Hack(zhongzc): If we have just become leader (writable), persist any backfilled metadata.
        let backfill_hook_payload = self.persist_backfilled_metadata_locked(&mut manager).await;

        drop(manager);

        // Merge both payloads so consumers see the complete set of actions in
        // one notification. The lock is released, so it's safe to fire.
        let merged = match (hook_payload, backfill_hook_payload) {
            (Some(staging), Some(backfill)) => Some(staging.merge(backfill)),
            (Some(payload), None) => Some(payload),
            (None, Some(payload)) => Some(payload),
            (None, None) => None,
        };

        if let Some(pending) = merged {
            pending.fire().await;
        }

        Ok(())
    }

    /// Persists the metadata backfilled when the region was opened if the manifest lacks it.
    /// A catchup cannot do it for a region that [awaits its Leader
    /// lease](ManifestContext::awaits_leader_lease), so the worker does once the region is a
    /// writable leader with its lease. Returns false if it found the region not a writable
    /// leader with its Leader lease, so it could not tell whether the metadata needs persisting.
    pub(crate) async fn persist_backfilled_metadata(&self) -> bool {
        let mut manager = self.manifest_ctx.manifest_manager.write().await;
        if !self.may_finish_deferred_lease_work() {
            return false;
        }
        // Checked once: a change the worker starts now cannot commit before the lock is dropped.
        let pending = self.commit_backfilled_metadata_locked(&mut manager).await;
        // The hook may read the manifest.
        drop(manager);
        if let Some(pending) = pending {
            pending.fire().await;
        }
        true
    }

    /// Returns whether the region is a writable leader with its Leader lease, which may run the
    /// work due once it is one.
    pub(crate) fn may_finish_deferred_lease_work(&self) -> bool {
        self.state() == RegionRoleState::Leader(RegionLeaderState::Writable)
            && !self.manifest_ctx.awaits_leader_lease()
    }

    /// Persists the backfilled metadata of a writable leader that may commit, under the
    /// caller's manifest lock. Returns the hook to fire after the lock is dropped.
    async fn persist_backfilled_metadata_locked(
        &self,
        manager: &mut RwLockWriteGuard<'_, RegionManifestManager>,
    ) -> Option<PendingManifestHook> {
        if !self.may_finish_deferred_lease_work() {
            return None;
        }
        self.commit_backfilled_metadata_locked(manager).await
    }

    /// Commits the backfilled metadata if the manifest lacks it, under the caller's manifest lock,
    /// without checking the state. Returns the hook to fire after the lock is dropped.
    async fn commit_backfilled_metadata_locked(
        &self,
        manager: &mut RwLockWriteGuard<'_, RegionManifestManager>,
    ) -> Option<PendingManifestHook> {
        // Persist backfilled metadata if manifest is missing fields (e.g., partition_expr).
        // Only the metadata and the format are kept, so the memtables of the version are not
        // pinned across the write below.
        let (current_meta, sst_format) = {
            let current_version = self.version();
            (
                current_version.metadata.clone(),
                current_version.options.sst_format.unwrap_or_default(),
            )
        };
        if manager.manifest().metadata.partition_expr.is_some()
            || current_meta.partition_expr.is_none()
        {
            return None;
        }
        let action = RegionMetaAction::Change(RegionChange {
            metadata: current_meta,
            sst_format,
            append_mode: None,
        });
        let action_list = RegionMetaActionList::with_action(action);
        match self
            .manifest_ctx
            .update_locked(manager, action_list, false)
            .await
        {
            Ok(pending) => {
                info!(
                    "Successfully persisted backfilled metadata for region {}, version: {}",
                    self.region_id,
                    pending.version()
                );
                Some(pending)
            }
            Err(e) => {
                warn!(e; "Failed to persist backfilled metadata for region {}", self.region_id);
                None
            }
        }
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

    /// Returns the current manifest byte count without computing other region statistics.
    pub(crate) fn manifest_size(&self) -> u64 {
        self.stats.total_manifest_size()
    }

    /// Returns the region statistic.
    pub(crate) fn region_statistic(&self) -> RegionStatistic {
        let version = self.version();
        let memtables = &version.memtables;
        let memtable_usage = (memtables.mutable_usage() + memtables.immutables_usage()) as u64;

        let sst_usage = version.ssts.owned_sst_usage(self.region_id);
        let index_usage = version.ssts.owned_index_usage(self.region_id);
        let flushed_entry_id = version.flushed_entry_id;

        let wal_usage = self.estimated_wal_usage(memtables.decoded_bytes() as u64);
        let manifest_usage = self.manifest_size();
        let num_rows = version.ssts.owned_num_rows(self.region_id) + version.memtables.num_rows();
        let num_files = version.ssts.owned_num_files(self.region_id);
        let manifest_version = self.stats.manifest_version();
        let file_removed_cnt = self.stats.file_removed_cnt();

        let time_range = match (version.ssts.time_range(), version.memtables.time_range()) {
            (Some((sst_min, sst_max)), Some((mem_min, mem_max))) => {
                Some((sst_min.min(mem_min), sst_max.max(mem_max)))
            }
            (range, None) | (None, range) => range,
        };

        let topic_latest_entry_id = self.topic_latest_entry_id.load(Ordering::Relaxed);
        let written_bytes = self.region_stats.written_bytes.load(Ordering::Relaxed);
        let query_cpu_time = self.region_stats.query_cpu_time.load(Ordering::Relaxed);
        let query_scanned_bytes = self
            .region_stats
            .query_scanned_bytes
            .load(Ordering::Relaxed);

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
            ingested_parquet_bytes: self
                .region_stats
                .ingested_parquet_bytes
                .load(Ordering::Relaxed),
            query_cpu_time,
            query_scanned_bytes,
            min_timestamp: time_range.map(|(min, _)| min),
            max_timestamp: time_range.map(|(_, max)| max),
            has_pending_tails: version.has_pending_tails(),
        }
    }

    /// Estimated WAL size in bytes.
    /// Use the decoded size of the memtable rows to estimate the size of wal.
    fn estimated_wal_usage(&self, memtable_decoded_bytes: u64) -> u64 {
        ((memtable_decoded_bytes as f32) * ESTIMATED_WAL_FACTOR) as u64
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

    /// Returns the region info entry of the region.
    pub(crate) fn region_info_entry(&self, node_id: Option<u64>) -> RegionInfoEntry {
        let region_id = self.region_id;
        let version = self.version();
        let state = self.state();
        let role = self.region_role();
        let region_options = serde_json::to_string(&version.options)
            .unwrap_or_else(|err| serde_json::json!({ "error": err.to_string() }).to_string());
        let sst_format = match version.options.sst_format.unwrap_or_default() {
            crate::sst::FormatType::Narthex => "narthex",
            crate::sst::FormatType::PrimaryKey => "primary_key",
            crate::sst::FormatType::Flat => "flat",
        }
        .to_string();

        RegionInfoEntry {
            region_id,
            table_id: region_id.table_id(),
            region_number: region_id.region_number(),
            region_group: region_id.region_group(),
            region_sequence: region_id.region_sequence(),
            state: state.as_str().to_string(),
            role: role.to_string(),
            writable: self.is_writable(),
            committed_sequence: self.find_committed_sequence(),
            flushed_sequence: Some(self.flushed_sequence()).filter(|sequence| *sequence > 0),
            manifest_version: self.stats.manifest_version(),
            compaction_time_window: version
                .compaction_time_window
                .map(|duration| humantime::format_duration(duration).to_string()),
            region_options,
            sst_format,
            node_id,
        }
    }

    /// Returns the SST entries of the region.
    pub(crate) async fn manifest_sst_entries(&self) -> Vec<ManifestSstEntry> {
        self.manifest_sst_stream(MetadataFilter::default(), ManifestSstProjection::new(None))
            .collect()
            .await
    }

    /// Traverses manifest snapshots without copying the file maps. Staging metadata
    /// overrides the normal manifest for the same file.
    pub(crate) fn manifest_sst_stream(
        &self,
        filters: MetadataFilter,
        projection: ManifestSstProjection,
    ) -> BoxStream<'_, ManifestSstEntry> {
        Box::pin(async_stream::stream! {
            let table_dir = self.table_dir();
            let path_type = self.access_layer.path_type();
            let visible_files = self.manifest_visible_file_ids(projection);
            let (manifest, staging) = {
                let manager = self.manifest_ctx.manifest_manager.read().await;
                (manager.manifest(), manager.staging_manifest())
            };
            // A region that left staging without a commit may still hold its staged files.
            let staging = staging.filter(|_| self.in_staging_session());
            let normal_files = manifest.files.values().map(|meta| (false, meta));
            let staging_files = staging.iter().flat_map(|s| s.files.values()).map(|meta| (true, meta));
            for (index, (is_staging, meta)) in normal_files.chain(staging_files).enumerate() {
                // A selective filter may otherwise traverse millions of files in
                // a single poll without yielding any rows.
                if index > 0 && index % 1024 == 0 {
                    tokio::task::yield_now().await;
                }
                if !is_staging && staging.as_ref().is_some_and(|s| s.files.contains_key(&meta.file_id)) {
                    continue;
                }
                if !manifest_file_matches(meta, &filters) {
                    continue;
                }
                // Paths describe the creating region, not the referring region.
                // Construct them only when a path predicate or projection needs them.
                let file_path = OnceCell::new();
                if !filters.matches(|column| match column {
                    sst_entry::FILE_PATH => Some(file_path.get_or_init(|| {
                        ScalarValue::Utf8(Some(meta.file_path(table_dir, path_type)))
                    })),
                    _ => None,
                }) {
                    continue;
                }
                let region_id = self.region_id;
                let origin_region_id = meta.region_id;
                let (index_version, index_file_path, index_file_size) = if meta.index_file_size > 0
                {
                    let index_file_path = projection.index_file_path.then(|| index_file_path(table_dir, meta.index_id(), path_type));
                    (
                        meta.index_version,
                        index_file_path,
                        Some(meta.index_file_size),
                    )
                } else {
                    (0, None, None)
                };
                let visible = visible_files.as_ref().is_some_and(|files| files.contains(&meta.file_id));
                let file_path = if projection.file_path {
                    match file_path.into_inner() {
                        Some(ScalarValue::Utf8(Some(path))) => path,
                        _ => meta.file_path(table_dir, path_type),
                    }
                } else {
                    String::new()
                };
                yield ManifestSstEntry {
                    table_dir: if projection.table_dir { table_dir.to_string() } else { String::new() },
                    region_id,
                    table_id: region_id.table_id(),
                    region_number: region_id.region_number(),
                    region_group: region_id.region_group(),
                    region_sequence: region_id.region_sequence(),
                    file_id: if projection.file_id { meta.file_id.to_string() } else { String::new() },
                    index_version,
                    level: meta.level,
                    file_path,
                    file_size: meta.file_size,
                    max_row_group_uncompressed_size: meta.max_row_group_uncompressed_size,
                    index_file_path,
                    index_file_size,
                    num_rows: meta.num_rows,
                    num_row_groups: meta.num_row_groups,
                    num_series: Some(meta.num_series),
                    min_ts: meta.time_range.0,
                    max_ts: meta.time_range.1,
                    sequence: meta.sequence.map(|s| s.get()),
                    partition_expr: if projection.partition_expr { meta.partition_expr.as_ref().map(ToString::to_string) } else { None },
                    origin_region_id,
                    node_id: None,
                    visible,
                    primary_key_min: if projection.primary_key_min { meta.primary_key_min.clone() } else { None },
                    primary_key_max: if projection.primary_key_max { meta.primary_key_max.clone() } else { None },
                };
            }
        })
    }

    /// Copies only file IDs so a paused inspection cannot delay SST purging.
    /// Hidden residual columns are included in the query's projection.
    fn manifest_visible_file_ids(
        &self,
        projection: ManifestSstProjection,
    ) -> Option<HashSet<FileId>> {
        projection.visible.then(|| {
            self.version()
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files().map(|file| file.file_id().file_id()))
                .collect()
        })
    }

    /// Returns whether the current read version contains any SSTs.
    pub fn has_ssts(&self) -> bool {
        self.version()
            .ssts
            .levels()
            .iter()
            .any(|level| !level.files.is_empty())
    }

    /// Returns the file metas of the region by file ids.
    pub async fn file_metas(&self, file_ids: &[FileId]) -> Vec<Option<FileMeta>> {
        let manifest_files = self.manifest_ctx.manifest().await.files.clone();

        file_ids
            .iter()
            .map(|file_id| {
                manifest_files
                    .get(file_id)
                    .map(|meta| meta.as_ref().clone())
            })
            .collect::<Vec<_>>()
    }

    /// Returns all live SST file metas and the normal manifest version, merging in the staging
    /// manifest while the region is in a staging session.
    ///
    /// In a staging session (e.g. during region copy/migration), the authoritative live file set
    /// lives in the staging manifest, so this method merges both — matching the semantics of
    /// [`manifest_sst_entries`]. Outside a session it ignores staged files, since a region that
    /// left staging may still hold them.
    pub async fn all_manifest_files(&self) -> (Vec<FileMeta>, ManifestVersion) {
        let manifest = self.manifest_ctx.manifest().await;
        let staging = self
            .manifest_ctx
            .staging_manifest()
            .await
            .filter(|_| self.in_staging_session())
            .map(|m| m.files.clone());

        let files = match staging {
            Some(staging_files) => {
                let merged = manifest
                    .files
                    .clone()
                    .into_iter()
                    .chain(staging_files)
                    .collect::<std::collections::HashMap<_, _>>();
                merged
                    .into_values()
                    .map(|meta| meta.as_ref().clone())
                    .collect()
            }
            None => manifest
                .files
                .values()
                .map(|meta| meta.as_ref().clone())
                .collect(),
        };

        (files, manifest.manifest_version)
    }

    /// Commits the staged edits as one manifest version and ends staging (see
    /// [Self::end_staging_locked]) under the manifest write lock `manager`, then drops the lock,
    /// awaits `drain_stalled_writes` and clears the staging directory. The drain waits for the
    /// worker to handle the writes it stalled, and the worker may be waiting for the lock, so the
    /// drain is never awaited under the lock.
    /// Returns the receipt of the commit, as [Self::end_staging_locked] does.
    pub(crate) async fn end_staging(
        &self,
        mut manager: RwLockWriteGuard<'_, RegionManifestManager>,
        drain_stalled_writes: impl Future<Output = ()>,
    ) -> Result<Option<PendingManifestHook>> {
        let result = self.end_staging_locked(&mut manager).await;
        drop(manager);

        // The worker cannot be awaited under the lock, and its writes need not wait for the
        // slower clear of the staging directory.
        drain_stalled_writes.await;
        let pending = result?;
        self.clear_ended_staging().await;
        Ok(pending)
    }

    /// Clears the staging directory after staging ended, unless the region was closed (it may be
    /// open again by now) or a new staging has begun.
    async fn clear_ended_staging(&self) {
        let mut manager = self.manifest_ctx.manifest_manager.write().await;
        if manager.is_stopped() || self.is_enter_staging() || self.in_staging_session() {
            return;
        }
        if let Err(e) = manager.clear_staging_manifest_and_dir().await {
            error!(e; "Failed to clear staging manifest dir for region {}", self.region_id);
        }
    }

    /// Commits the staged edits as one manifest version and ends staging.
    ///
    /// The caller must hold the manifest write lock and pass it via `manager`. The commit runs in
    /// [RegionLeaderState::EndingStaging], which stalls writes and holds role changes back (see
    /// [ManifestContext::leave_ending_staging] for the state it leaves for). If the region stays
    /// staging, its staged edits are kept for a retry and the error is retryable; otherwise they
    /// are dropped.
    ///
    /// Returns `Ok(None)` when there was nothing to commit.
    ///
    /// **Important:** [`fire`](PendingManifestHook::fire) the receipt only after
    /// dropping the lock — the hook may read the manifest and deadlock otherwise.
    pub(crate) async fn end_staging_locked(
        &self,
        manager: &mut RwLockWriteGuard<'_, RegionManifestManager>,
    ) -> Result<Option<PendingManifestHook>> {
        let current_state = self.manifest_ctx.current_state();
        ensure!(
            current_state == RegionRoleState::Leader(RegionLeaderState::Staging),
            RegionStateSnafu {
                region_id: self.region_id,
                state: current_state,
                expect: RegionRoleState::Leader(RegionLeaderState::Staging),
            }
        );

        let Some(staged_actions) = manager.staged_actions() else {
            info!(
                "No staged actions for region {}, exiting staging mode without changes",
                self.region_id
            );
            self.exit_staging()?;
            return Ok(None);
        };
        let expect_change = staged_actions.actions.iter().any(|a| a.is_change());
        let expect_partition_expr_change = staged_actions
            .actions
            .iter()
            .any(|a| a.is_partition_expr_change());
        let expect_edit = staged_actions.actions.iter().any(|a| a.is_edit());
        ensure!(
            !(expect_change && expect_partition_expr_change),
            UnexpectedSnafu {
                reason: "unexpected both change and partition expr change actions in staged actions"
            }
        );
        ensure!(
            expect_change || expect_partition_expr_change,
            UnexpectedSnafu {
                reason: "expect a change or partition expr change action in staged actions"
            }
        );
        ensure!(
            expect_edit,
            UnexpectedSnafu {
                reason: "expect an edit action in staged actions"
            }
        );

        if let Some(RegionMetaAction::Change(change)) =
            staged_actions.actions.iter().find(|a| a.is_change())
        {
            // In staging exit we only allow metadata-only updates. A `Change`
            // action is accepted only when column definitions are unchanged;
            // otherwise it is treated as a schema change and rejected.
            let current_column_metadatas = &self.version().metadata.column_metadatas;
            ensure!(
                change.metadata.column_metadatas == *current_column_metadatas,
                UnexpectedSnafu {
                    reason: "change action alters column metadata in staging exit"
                }
            );
        }

        self.manifest_ctx
            .ensure_leader_lease_or_downgrading(self.region_id)?;
        let ending_started_at = self.manifest_ctx.begin_ending_staging(self.region_id)?;
        let mut result = self.manifest_ctx.commit_staged_locked(manager).await;
        if let Ok((pending, committed)) = &mut result {
            info!(
                "Successfully committed staged manifests for region {}, new version: {}",
                self.region_id,
                pending.version()
            );
            // Apply the committed changes to in-memory version control
            let (staged_partition_expr_change, staged_change, staged_edit) =
                std::mem::take(committed).split_region_change_and_edit();
            if let Some(change) = staged_partition_expr_change {
                let mut new_metadata = self.version().metadata.as_ref().clone();
                new_metadata.set_partition_expr(change.partition_expr);
                self.version_control.alter_metadata(new_metadata.into());
            }
            if let Some(change) = staged_change {
                self.version_control.alter_metadata(change.metadata);
            }
            self.version_control
                .apply_edit(Some(staged_edit), &[], self.file_purger.clone());
        }
        let result = result.map(|(pending, _)| pending);
        let next_state =
            self.manifest_ctx
                .leave_ending_staging(self.region_id, ending_started_at, &result);
        if next_state != RegionRoleState::Leader(RegionLeaderState::Staging) {
            // Only a retry commits the staged actions. The listings gate on the state and do not
            // rely on this.
            manager.unset_staging_manifest();
        }

        match result {
            // A retry of the request commits the kept edits.
            Err(e) if next_state == RegionRoleState::Leader(RegionLeaderState::Staging) => {
                Err(Arc::new(e)).context(EndStagingSnafu {
                    region_id: self.region_id,
                })
            }
            result => result.map(Some),
        }
    }

    pub fn expected_partition_expr_version(&self) -> u64 {
        if self.is_staging() {
            self.manifest_ctx
                .staging_partition_info()
                .as_ref()
                .map(|info| info.partition_rule_version)
                .unwrap_or_default()
        } else {
            self.version().metadata.partition_expr_version
        }
    }

    /// Returns whether writes should be rejected for this region in staging mode.
    pub(crate) fn reject_all_writes_in_staging(&self) -> bool {
        if !self.is_staging() {
            return false;
        }
        self.manifest_ctx
            .staging_partition_info()
            .as_ref()
            .map(|info| {
                matches!(
                    info.partition_directive,
                    StagingPartitionDirective::RejectAllWrites
                )
            })
            .unwrap_or(false)
    }
}

impl Drop for MitoRegion {
    fn drop(&mut self) {
        self.remove_region_metrics();
    }
}

/// Result of publishing rebuilt index metadata to the manifest.
#[derive(Debug)]
pub(crate) enum IndexPublication {
    /// The index metadata was committed.
    Committed {
        manifest_version: ManifestVersion,
        file_meta: FileMeta,
    },
    /// The build no longer matches the current manifest.
    Stale(IndexPublicationStale),
}

/// Result of publishing an edit of the pending tails, a replay's result or a discard, to the
/// manifest.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TailPublication {
    /// The edit was committed.
    Committed(ManifestVersion),
    /// The region is altering or applying an edit, or, without conditional publication, a
    /// replay's earlier update may be saved without being applied.
    Busy,
    /// The region is no longer a writable leader, or its manifest no longer records the
    /// expected tails.
    Stale,
}

/// Who publishes an edit of the pending tails.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TailsEditor {
    /// A tail replay, which commits its own uncertain update again.
    Replay,
    /// A discard of one node's tails.
    Discard,
}

/// Why an index publication became stale.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum IndexPublicationStale {
    /// The source SST or region incarnation is no longer publishable.
    SourceChanged,
    /// The SST still matches, but the schema generation changed.
    SchemaChanged,
}

/// Manifest state an index build is based on.
///
/// Both fields must still match when the rebuilt index is published. The file
/// metadata identifies the exact SST generation, while the schema version
/// identifies the exact index definition used by the builder.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexBuildSource {
    pub(crate) file_meta: FileMeta,
    pub(crate) schema_version: u64,
}

impl IndexBuildSource {
    pub(crate) fn new(file_meta: FileMeta, schema_version: u64) -> Self {
        Self {
            file_meta,
            schema_version,
        }
    }
}

/// What [ManifestContext::promote_unless_downgrade_started_after] did with a Leader lease.
pub(crate) struct LeaderLeasePromotion {
    /// Whether the promotion applied; see the function.
    pub(crate) promoted: bool,
    /// How long the region awaited its Leader lease, if this lease ended the wait, as
    /// [ManifestContext::record_leader_lease] returns it.
    pub(crate) ended_wait: Option<Duration>,
    /// The state the promotion replaced, None if it left the state unchanged.
    pub(crate) replaced: Option<RegionRoleState>,
}

/// What [ManifestContext::promote_unless_reload_pending] did.
struct Promotion {
    /// Whether it promoted the region; see the function.
    promoted: bool,
    /// The state the promotion replaced, as [ManifestContext::set_role] returns it.
    replaced: Option<RegionRoleState>,
}

/// Context to update the region manifest.
#[derive(Debug)]
pub(crate) struct ManifestContext {
    /// Manager to maintain manifest for this region. Logical writes go through
    /// [`update_locked`](Self::update_locked) (or an [`update_manifest`](Self::update_manifest)
    /// variant; a replay of pending tails makes the same write unchecked, and the end of staging
    /// commits through [`commit_staged_locked`](Self::commit_staged_locked)) so they produce a
    /// [`PendingManifestHook`].
    pub(crate) manifest_manager: tokio::sync::RwLock<RegionManifestManager>,
    /// Whether [`Self::manifest_manager`] publishes the manifest by conditional create, which is
    /// fixed when the manager is opened.
    conditional_publication: bool,
    /// The state of the region. The region checks the state before updating
    /// manifest.
    state: AtomicCell<RegionRoleState>,
    /// Decides whether a promotion to Leader applies. The lock covers checking or changing it and
    /// the role change.
    promotion_fence: Mutex<PromotionFence>,
    /// When a catchup last made the region writable, in the unit of [Deadline], until a Leader
    /// lease of a heartbeat sent after it applies (or a StagingLeader lease, on a node whose
    /// writes need a lease); 0 otherwise. While it is set, the region keeps
    /// the unread part of another node's listed tails.
    catchup_promoted_at: AtomicU64,
    /// Whether a tail replay's manifest update has an uncertain result: it may be saved in
    /// storage without being applied to this instance's manifest. Tracked only without
    /// conditional publication, where a later update takes a new version instead of
    /// conflicting with a saved one. A value left by an ended replay task keeps discards busy
    /// until a replay of this instance publishes or the region is reopened.
    replay_publication_uncertain: AtomicBool,
    /// Deadline of the region's latest Leader or StagingLeader lease from the metasrv, which is how
    /// long the Leader role stays valid. The engine extends it from those leases, and writes check
    /// it.
    pub(crate) leader_deadline: Deadline,
    /// The clock of [`Self::leader_deadline`].
    node_lease: Arc<NodeLease>,
    /// Partition info of the region in staging mode.
    ///
    /// During the staging mode, the region metadata in [`VersionControlRef`] is not updated,
    /// so we need to store the partition info separately.
    staging_partition_info: Mutex<Option<StagingPartitionInfo>>,
    /// Optional region hook for observing manifest mutations.
    hook: Option<RegionHookRef>,
    /// Notifications that the region became writable sent to its worker.
    #[cfg(test)]
    pub(crate) writable_notifications: std::sync::atomic::AtomicUsize,
}

impl ManifestContext {
    pub(crate) fn new(
        manager: RegionManifestManager,
        state: RegionRoleState,
        hook: Option<RegionHookRef>,
        node_lease: Arc<NodeLease>,
    ) -> Self {
        ManifestContext {
            conditional_publication: manager.is_conditional_publication(),
            manifest_manager: tokio::sync::RwLock::new(manager),
            state: AtomicCell::new(state),
            promotion_fence: Mutex::new(PromotionFence {
                downgrade_started_at: Instant::now(),
                reload_pending: false,
                catching_up: false,
                stopped: false,
            }),
            catchup_promoted_at: AtomicU64::new(0),
            replay_publication_uncertain: AtomicBool::new(false),
            leader_deadline: Deadline::default(),
            node_lease,
            staging_partition_info: Mutex::new(None),
            hook,
            #[cfg(test)]
            writable_notifications: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Returns the region hook if one is registered.
    pub(crate) fn hook(&self) -> Option<RegionHookRef> {
        self.hook.clone()
    }

    pub(crate) fn staging_partition_info(&self) -> Option<StagingPartitionInfo> {
        self.staging_partition_info.lock().unwrap().clone()
    }

    pub(crate) fn set_staging_partition_info(&self, staging_partition_info: StagingPartitionInfo) {
        let mut current = self.staging_partition_info.lock().unwrap();
        debug_assert!(current.is_none());
        *current = Some(staging_partition_info);
    }

    fn clear_staging_partition_info(&self) {
        *self.staging_partition_info.lock().unwrap() = None;
    }

    pub(crate) fn exit_staging(
        &self,
        region_id: RegionId,
        next_state: RegionRoleState,
    ) -> Result<()> {
        self.state
            .compare_exchange(
                RegionRoleState::Leader(RegionLeaderState::Staging),
                next_state,
            )
            .map_err(|actual| {
                RegionStateSnafu {
                    region_id,
                    state: actual,
                    expect: RegionRoleState::Leader(RegionLeaderState::Staging),
                }
                .build()
            })?;
        self.clear_staging_partition_info();
        Ok(())
    }

    /// Switches a staging region to [RegionLeaderState::EndingStaging] and returns the instant
    /// it switched.
    ///
    /// It holds the lock of [Self::begin_downgrade], so a downgrade either has left staging
    /// already, or begins after the returned instant.
    pub(crate) fn begin_ending_staging(&self, region_id: RegionId) -> Result<Instant> {
        let _fence = self.promotion_fence.lock().unwrap();
        self.state
            .compare_exchange(
                RegionRoleState::Leader(RegionLeaderState::Staging),
                RegionRoleState::Leader(RegionLeaderState::EndingStaging),
            )
            .map_err(|actual| {
                RegionStateSnafu {
                    region_id,
                    state: actual,
                    expect: RegionRoleState::Leader(RegionLeaderState::Staging),
                }
                .build()
            })?;
        Ok(Instant::now())
    }

    /// Leaves [RegionLeaderState::EndingStaging], entered at `ending_started_at`, after the end
    /// commit returned `result`. Returns the state the region leaves it for, which is in order:
    /// - Follower if the commit conflicted with another writer or failed on an expired lease;
    /// - Downgrading if a downgrade began meanwhile;
    /// - Follower if the Leader lease has expired;
    /// - Writable if the commit succeeded;
    /// - Staging otherwise, keeping the staging partition info for a retry.
    pub(crate) fn leave_ending_staging<T>(
        &self,
        region_id: RegionId,
        ending_started_at: Instant,
        result: &Result<T>,
    ) -> RegionRoleState {
        let fence = self.promotion_fence.lock().unwrap();
        let lease_expired = self.node_lease.is_enabled() && self.is_leader_lease_expired();
        let next_state = match result {
            Err(Error::ManifestVersionConflict { .. } | Error::RegionLeaseExpired { .. }) => {
                RegionRoleState::Follower
            }
            _ if fence.downgrade_started_at > ending_started_at => {
                RegionRoleState::Leader(RegionLeaderState::Downgrading)
            }
            _ if lease_expired => RegionRoleState::Follower,
            Ok(_) => RegionRoleState::Leader(RegionLeaderState::Writable),
            Err(_) => RegionRoleState::Leader(RegionLeaderState::Staging),
        };
        // Role changes leave a region that is ending staging alone.
        self.state.store(next_state);
        if next_state != RegionRoleState::Leader(RegionLeaderState::Staging) {
            self.clear_staging_partition_info();
        }
        info!(
            "Region {} ended staging, next state: {:?}",
            region_id, next_state
        );
        next_state
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

    /// Waits for the in-flight checkpoint of a stopped manager. A stopped manager starts no
    /// checkpoint, so this instance no longer writes the manifest directory afterwards.
    ///
    /// Callers must not hold the manifest lock: this takes its read lock, and the lock is not
    /// reentrant. Under the lock, use [RegionManifestManager::wait_for_pending_checkpoint]
    /// instead. The wait itself holds no lock.
    pub(crate) async fn wait_for_checkpoint_after_stop(&self) {
        let done = {
            let manager = self.manifest_manager.read().await;
            debug_assert!(manager.is_stopped());
            manager.pending_checkpoint_completion()
        };
        done.await;
    }

    /// Returns the installed manifest version if the storage holds a newer one. The version is
    /// read under the manifest read lock, which is not held across the listing, so a caller
    /// rechecks it under the write lock before acting on it. It is the installed version, not
    /// `last_version`, which a failed commit leaves ahead of it.
    pub(crate) async fn installed_version_if_updated(&self) -> Result<Option<ManifestVersion>> {
        let (store, installed) = {
            let manager = self.manifest_manager.read().await;
            (manager.store(), manager.manifest().manifest_version)
        };
        // Lists without the read lock: held across the listing, it would let a queued writer
        // block every later reader.
        Ok(store
            .has_version_after(installed)
            .await?
            .then_some(installed))
    }

    /// Fences the region for a reload, see [Self::fence_for_reload], if it is still a Writable
    /// leader at `expected_version`. Returns whether it fenced the region.
    pub(crate) async fn fence_for_reload_if_unchanged(
        &self,
        manager: &mut RegionManifestManager,
        region_id: RegionId,
        expected_version: ManifestVersion,
    ) -> bool {
        let state = self.state.load();
        let version = manager.manifest().manifest_version;
        if state != RegionRoleState::Leader(RegionLeaderState::Writable)
            || version != expected_version
        {
            info!(
                "Region {region_id} is {state:?} at manifest version {version}, not a writable leader at version {expected_version}, not fencing it"
            );
            return false;
        }
        self.fence_for_reload(manager, region_id).await;
        true
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
        self.update_manifest_with_state_check(action_list, is_staging, |current_state, region_id| {
            // If expect_state is not downgrading, the current state must be either `expect_state` or downgrading.
            //
            // A downgrading leader rejects user writes but still allows
            // flushing the memtable and updating the manifest.
            if expect_state != RegionLeaderState::Downgrading {
                if current_state == RegionRoleState::Leader(RegionLeaderState::Downgrading) {
                    info!(
                        "Region {} is in downgrading leader state, updating manifest. Expect state is {:?}",
                        region_id, expect_state
                    );
                }
                ensure!(
                    current_state == RegionRoleState::Leader(expect_state)
                        || current_state == RegionRoleState::Leader(RegionLeaderState::Downgrading),
                    UpdateManifestSnafu {
                        region_id,
                        state: current_state,
                    }
                );
            } else {
                ensure!(
                    current_state == RegionRoleState::Leader(expect_state),
                    RegionStateSnafu {
                        region_id,
                        state: current_state,
                        expect: RegionRoleState::Leader(expect_state),
                    }
                );
            }

            Ok(())
        })
        .await
    }

    /// Updates the manifest for compaction.
    ///
    /// Compaction may finish while a direct external region edit is in the transient
    /// `Editing` state. Direct external edits can remove files both when followers
    /// apply sync-region metadata and when a writable leader performs a direct edit
    /// such as `edit_region()`. Allowing compaction to publish in `Editing` is still
    /// safe because publication happens under the manifest write lock and compaction
    /// rechecks that its input files are still valid before committing.
    ///
    /// This intentionally writes to the normal manifest path (`is_staging = false`).
    /// Entering staging cancels or waits for active compactions before switching the
    /// region to `Staging`, so a compaction that started before staging still finishes
    /// against the normal manifest. Even if a manual compaction is requested while the
    /// region is already staging, compaction only sees SSTs in the normal visible
    /// region version; SSTs from staging manifests are not applied to region version
    /// control until staging exits successfully.
    pub(crate) async fn update_manifest_for_compaction(
        &self,
        action_list: RegionMetaActionList,
    ) -> Result<ManifestVersion> {
        self.update_manifest_with_state_check(action_list, false, |current_state, region_id| {
            ensure!(
                matches!(
                    current_state,
                    RegionRoleState::Leader(RegionLeaderState::Writable)
                        | RegionRoleState::Leader(RegionLeaderState::Editing)
                        | RegionRoleState::Leader(RegionLeaderState::Downgrading)
                ),
                UpdateManifestSnafu {
                    region_id,
                    state: current_state,
                }
            );

            Ok(())
        })
        .await
    }

    /// Publish against the same truncate boundary and the watermarks used to select inputs.
    pub(crate) async fn publish_series_indexes(
        &self,
        edit: &crate::manifest::action::RegionEdit,
        truncated: Option<crate::wal::EntryId>,
        expected: &HashMap<i64, u64>,
    ) -> Result<()> {
        let mut manager = self.manifest_manager.write().await;
        let manifest = manager.manifest();
        let region_id = manifest.metadata.region_id;
        ensure!(
            !manager.is_stopped()
                && self.state.load() == RegionRoleState::Leader(RegionLeaderState::Writable)
                && manifest.truncated_entry_id == truncated
                && manifest.pending_tails.is_empty(),
            RegionBusySnafu { region_id }
        );
        // A competing merge may replace inputs without advancing the watermark.
        for id in &edit.series_indexes_to_remove {
            ensure!(
                manifest
                    .series_indexes
                    .get(&id.file_id())
                    .is_some_and(|file| file.file_id() == *id),
                RegionBusySnafu { region_id }
            );
        }
        let mut current = HashMap::<i64, u64>::new();
        for w in manifest
            .series_indexes
            .values()
            .filter_map(|f| f.series_window())
        {
            let seq = current.entry(w.start).or_default();
            *seq = (*seq).max(w.max_sequence);
        }
        for window in edit
            .series_indexes_to_add
            .iter()
            .filter_map(FileMeta::series_window)
        {
            ensure!(
                manifest
                    .series_window_seconds
                    .is_none_or(|width| width == window.end - window.start)
                    && current.get(&window.start) == expected.get(&window.start),
                RegionBusySnafu { region_id }
            );
        }
        let actions = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone()));
        match self.update_locked(&mut manager, actions, false).await {
            Ok(pending) => {
                self.unlock_and_fire(manager, pending).await;
            }
            Err(error) => {
                if error.may_have_persisted_manifest_update() {
                    manager.stop().await;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    /// Conditionally publishes rebuilt index metadata for `source`.
    ///
    /// The source SST and schema-generation checks share the manifest write lock
    /// with the update, so a concurrent compaction, schema change, or another
    /// index build cannot commit between them.
    pub(crate) async fn update_manifest_for_index(
        &self,
        source: &IndexBuildSource,
        updated: FileMeta,
    ) -> Result<IndexPublication> {
        let manager = self.manifest_manager.write().await;
        let manifest = manager.manifest();
        let current_state = self.state.load();
        if !matches!(
            current_state,
            RegionRoleState::Leader(RegionLeaderState::Writable)
                | RegionRoleState::Leader(RegionLeaderState::Downgrading)
        ) || manager.is_stopped()
        {
            return Ok(IndexPublication::Stale(
                IndexPublicationStale::SourceChanged,
            ));
        }

        if manifest
            .files
            .get(&source.file_meta.file_id)
            .map(AsRef::as_ref)
            != Some(&source.file_meta)
        {
            return Ok(IndexPublication::Stale(
                IndexPublicationStale::SourceChanged,
            ));
        }

        if manifest.metadata.schema_version != source.schema_version {
            return Ok(IndexPublication::Stale(
                IndexPublicationStale::SchemaChanged,
            ));
        }

        // Only index metadata is allowed to change in this publication.
        let mut committed = source.file_meta.clone();
        committed.available_indexes = updated.available_indexes;
        committed.indexes = updated.indexes;
        committed.index_file_size = updated.index_file_size;
        committed.index_version = updated.index_version;

        let edit = crate::manifest::action::RegionEdit {
            files_to_add: vec![committed.clone()],
            files_to_remove: Vec::new(),
            timestamp_ms: Some(chrono::Utc::now().timestamp_millis()),
            flushed_sequence: None,
            flushed_entry_id: None,
            committed_sequence: None,
            compaction_time_window: None,
            ..Default::default()
        };
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
        let manifest_version = self.update_and_fire(manager, action_list, false).await?;

        Ok(IndexPublication::Committed {
            manifest_version,
            file_meta: committed,
        })
    }

    /// Publishes an edit of the pending tails `expected` by `editor`, if the region is still a
    /// writable leader whose manifest records exactly those tails.
    ///
    /// Without conditional publication, a discard is busy while a replay's earlier update has an
    /// uncertain result, since it may be in storage and the discard would record the replayed
    /// tails again. A replay's update is uncertain from before its write until it commits; an
    /// attempt that fails without saving anything keeps the uncertainty of the attempts before
    /// it. With conditional publication the next update reuses the version of an uncertain
    /// one, so a discard over a saved update conflicts and the region reloads instead.
    pub(crate) async fn update_manifest_for_tails(
        &self,
        expected: &[PendingTail],
        action_list: RegionMetaActionList,
        editor: TailsEditor,
    ) -> Result<TailPublication> {
        let mut manager = self.manifest_manager.write().await;
        match self.state.load() {
            RegionRoleState::Leader(RegionLeaderState::Writable) => {}
            RegionRoleState::Leader(RegionLeaderState::Altering | RegionLeaderState::Editing) => {
                return Ok(TailPublication::Busy);
            }
            _ => return Ok(TailPublication::Stale),
        }
        if manager.manifest().pending_tails != expected {
            return Ok(TailPublication::Stale);
        }
        let tracks_uncertainty = !manager.is_conditional_publication();
        let was_uncertain = match editor {
            // Set under the lock before the write, so no discard commits between a saved write
            // and its error.
            TailsEditor::Replay if tracks_uncertainty => self
                .replay_publication_uncertain
                .swap(true, Ordering::Relaxed),
            TailsEditor::Discard
                if tracks_uncertainty
                    && self.replay_publication_uncertain.load(Ordering::Relaxed) =>
            {
                return Ok(TailPublication::Busy);
            }
            _ => false,
        };
        // A region that awaits its Leader lease commits a replay of its tails, not a discard.
        let result = match editor {
            TailsEditor::Replay => {
                match self.write_locked(&mut manager, action_list, false).await {
                    Ok(pending) => Ok(self.unlock_and_fire(manager, pending).await),
                    Err(e) => Err(e),
                }
            }
            TailsEditor::Discard => self.update_and_fire(manager, action_list, false).await,
        };
        if editor == TailsEditor::Replay && tracks_uncertainty {
            match &result {
                Ok(_) => self
                    .replay_publication_uncertain
                    .store(false, Ordering::Relaxed),
                // This attempt saved nothing, but an earlier one may have.
                Err(e) if !e.may_have_persisted_manifest_update() => self
                    .replay_publication_uncertain
                    .store(was_uncertain, Ordering::Relaxed),
                Err(_) => {}
            }
        }
        Ok(TailPublication::Committed(result?))
    }

    /// Commits the empty edit a catchup makes before the region becomes writable, as the version
    /// after the manifest the region holds when it commits. A writer that committed other content
    /// at that version conflicts with it. Two catchups that start from the same manifest version
    /// commit the same content, so they are not told apart. Only a manifest published
    /// conditionally detects a conflict, so without conditional publication it commits nothing.
    ///
    /// The region commits as a Follower, which needs no valid Leader deadline, so the commit
    /// skips the deadline check. Like a commit of a region that awaits its Leader lease, it
    /// starts no checkpoint and applies only if no checkpoint covers its version. A conflict
    /// leaves the region and its manifest manager as they are: the next catchup reopens the
    /// region from the newer manifest. A stopped region commits nothing.
    ///
    /// Returns the receipt of the manifest hook, which the caller fires once it holds no manifest
    /// lock, or None if it committed nothing.
    pub(crate) async fn commit_catchup(&self) -> Result<Option<PendingManifestHook>> {
        let mut manager = self.manifest_manager.write().await;
        if !manager.is_conditional_publication() {
            return Ok(None);
        }
        self.commit_catchup_locked(&mut manager, RegionEdit::default())
            .await
            .map(Some)
    }

    /// Commits the catchup of a region whose manifest is published conditionally as
    /// [Self::commit_catchup] does, with `edit` and under the caller's manifest write lock. The
    /// caller decides whether the region's worker applies `edit` to the region's version.
    pub(crate) async fn commit_catchup_locked(
        &self,
        manager: &mut RegionManifestManager,
        edit: RegionEdit,
    ) -> Result<PendingManifestHook> {
        let region_id = manager.manifest().metadata.region_id;
        let state = self.state.load();
        ensure!(
            state == RegionRoleState::Follower,
            RegionStateSnafu {
                region_id,
                state,
                expect: RegionRoleState::Follower,
            }
        );
        ensure!(!self.is_reload_pending(), RegionBusySnafu { region_id });
        // A stop marks the region before it waits for this lock to stop the manager.
        ensure!(!self.is_stopped(), RegionStoppedSnafu { region_id });
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(edit));
        let action_list_for_hook = self.hook.as_ref().map(|_| action_list.clone());
        let version = manager
            .update_with_verification(action_list, false, false, || true)
            .await?;
        let pending = PendingManifestHook::new(
            region_id,
            action_list_for_hook,
            version,
            self.hook.clone(),
            false,
        );
        Ok(pending)
    }

    /// Performs a manifest write under a caller-held write lock and returns a
    /// [`PendingManifestHook`] to [`fire`](PendingManifestHook::fire) after
    /// dropping the lock. It is the checked entry of the shared write
    /// `write_locked`, which a replay of pending tails calls without the check;
    /// the end of staging commits through [`Self::commit_staged_locked`] instead.
    ///
    /// Fails an update that is not staged while the region [awaits its Leader
    /// lease](Self::awaits_leader_lease) and is not downgrading. Does not
    /// validate state or edit applicability — the caller must do so while still
    /// holding the lock (see `update_manifest_with_state_check` and
    /// `MitoRegion::persist_backfilled_metadata_locked`).
    pub(crate) async fn update_locked(
        &self,
        manager: &mut RegionManifestManager,
        action_list: RegionMetaActionList,
        is_staging: bool,
    ) -> Result<PendingManifestHook> {
        if !is_staging {
            self.ensure_leader_lease_or_downgrading(manager.manifest().metadata.region_id)?;
        }
        self.write_locked(manager, action_list, is_staging).await
    }

    /// Performs the manifest write of [Self::update_locked] without its Leader lease check.
    async fn write_locked(
        &self,
        manager: &mut RegionManifestManager,
        action_list: RegionMetaActionList,
        is_staging: bool,
    ) -> Result<PendingManifestHook> {
        let region_id = manager.manifest().metadata.region_id;
        // Clone before `action_list` is moved into `update_with_verification`
        // so the hook still sees what was written.
        let action_list_for_hook = self.hook.as_ref().map(|_| action_list.clone());
        // A lease that ends the wait during the write does not lift the checks below.
        let awaited_leader_lease = self.awaits_leader_lease();
        // The final flush of a downgrading leader must not start cleanup after the downgrade
        // checkpoint barrier, nor may a region that awaits its Leader lease and may not own the
        // region yet.
        let allow_checkpoint = !is_staging
            && self.state.load() != RegionRoleState::Leader(RegionLeaderState::Downgrading)
            && !awaited_leader_lease;
        let version = self
            .commit_locked(manager, async |manager| {
                manager
                    .update_with_verification(action_list, is_staging, allow_checkpoint, || {
                        awaited_leader_lease || self.must_verify_commit()
                    })
                    .await
            })
            .await?;

        Ok(PendingManifestHook::new(
            region_id,
            action_list_for_hook,
            version,
            self.hook.clone(),
            is_staging,
        ))
    }

    /// Commits the staged actions of `manager` as one version under a caller-held write lock.
    /// Returns a [`PendingManifestHook`] to [`fire`](PendingManifestHook::fire) after dropping
    /// the lock, and the committed actions.
    pub(crate) async fn commit_staged_locked(
        &self,
        manager: &mut RegionManifestManager,
    ) -> Result<(PendingManifestHook, RegionMetaActionList)> {
        let region_id = manager.manifest().metadata.region_id;
        let (version, committed) = self
            .commit_locked(manager, async |manager| {
                manager.commit_staged(|| self.must_verify_commit()).await
            })
            .await?;
        let actions_for_hook = self.hook.as_ref().map(|_| committed.clone());

        let pending = PendingManifestHook::new(
            region_id,
            actions_for_hook,
            version,
            self.hook.clone(),
            false,
        );
        Ok((pending, committed))
    }

    /// Runs the manifest commit `commit` under a caller-held write lock. A writer past its
    /// Leader deadline may no longer hold the region, so in conditional publication it sends
    /// nothing; a commit that finds a newer manifest fences the region (see
    /// [Self::fence_for_reload]).
    async fn commit_locked<T>(
        &self,
        manager: &mut RegionManifestManager,
        commit: impl AsyncFnOnce(&mut RegionManifestManager) -> Result<T>,
    ) -> Result<T> {
        let region_id = manager.manifest().metadata.region_id;
        let result = match self.ensure_commit_deadline(manager) {
            Ok(()) => commit(manager).await,
            Err(error) => Err(error),
        };
        if let Err(e) = &result {
            error!(e; "Failed to update manifest, region_id: {}", region_id);
            if matches!(e, Error::ManifestVersionConflict { .. }) {
                self.fence_for_reload(manager, region_id).await;
            }
        }
        result
    }

    /// Checks the leader deadline for a conditionally published manifest.
    pub(crate) fn ensure_commit_deadline(&self, manager: &RegionManifestManager) -> Result<()> {
        ensure!(
            !manager.is_conditional_publication() || !self.is_leader_lease_expired(),
            RegionLeaseExpiredSnafu {
                region_id: manager.manifest().metadata.region_id
            }
        );
        Ok(())
    }

    /// Returns whether a commit, once written, checks that no checkpoint covers its version: a
    /// region past its Leader deadline may no longer hold the region, and one that [awaits its
    /// Leader lease](Self::awaits_leader_lease) may not hold it yet, so either may have written a
    /// version name that checkpoint cleanup freed.
    fn must_verify_commit(&self) -> bool {
        self.is_leader_lease_expired() || self.awaits_leader_lease()
    }

    /// Returns whether the region's Leader deadline has passed. No deadline counts as passed.
    fn is_leader_lease_expired(&self) -> bool {
        self.node_lease.now() >= self.leader_deadline.get()
    }

    /// Fences a region whose manifest another writer has advanced, under the caller's manifest
    /// lock: later commits fail, and the region stays a Follower that no promotion changes until
    /// the worker replaces it with an instance reloaded from the new manifest.
    pub(crate) async fn fence_for_reload(
        &self,
        manager: &mut RegionManifestManager,
        region_id: RegionId,
    ) {
        manager.stop().await;
        // Marked before the role change, so a promotion in between cannot make it writable again.
        self.promotion_fence.lock().unwrap().reload_pending = true;
        self.set_role(RegionRole::Follower, region_id);
        warn!("Region {region_id} is fenced until it is reloaded");
    }

    /// Returns whether the region waits for a reload, see [Self::fence_for_reload].
    pub(crate) fn is_reload_pending(&self) -> bool {
        self.promotion_fence.lock().unwrap().reload_pending
    }

    /// Returns whether a catchup runs on this instance, see [Self::begin_catchup]. An instance a
    /// catchup replaced keeps the flag set, so it is read on the registered instance.
    pub(crate) fn is_catching_up(&self) -> bool {
        self.promotion_fence.lock().unwrap().catching_up
    }

    /// Ignores the promotions issued before now. The worker calls it on a reloaded instance, and
    /// on the replacement of a catchup again, when it replaces the old one, whose time a downgrade
    /// meanwhile raised instead.
    pub(crate) fn ignore_earlier_promotions(&self) {
        self.promotion_fence.lock().unwrap().downgrade_started_at = Instant::now();
    }

    /// Updates the manifest using a caller-held write lock, releases the lock,
    /// and then fires the manifest hook.
    ///
    /// Callers must perform state and applicability checks before calling this
    /// method so the checks and update remain in the same lock critical section.
    async fn update_and_fire(
        &self,
        mut manager: RwLockWriteGuard<'_, RegionManifestManager>,
        action_list: RegionMetaActionList,
        is_staging: bool,
    ) -> Result<ManifestVersion> {
        let pending = self
            .update_locked(&mut manager, action_list, is_staging)
            .await?;
        Ok(self.unlock_and_fire(manager, pending).await)
    }

    /// Releases the lock of the write that returned `pending`, then fires its hook.
    async fn unlock_and_fire(
        &self,
        manager: RwLockWriteGuard<'_, RegionManifestManager>,
        pending: PendingManifestHook,
    ) -> ManifestVersion {
        let region_id = manager.manifest().metadata.region_id;
        let version = pending.version();

        // Hook implementations may read the manifest or send region requests
        // that acquire this lock.
        drop(manager);

        if self.state.load() == RegionRoleState::Follower {
            warn!(
                "Region {} becomes follower while updating manifest which may cause inconsistency, manifest version: {version}",
                region_id
            );
        }

        pending.fire().await;
        version
    }

    async fn update_manifest_with_state_check(
        &self,
        action_list: RegionMetaActionList,
        is_staging: bool,
        check_state: impl FnOnce(RegionRoleState, RegionId) -> Result<()>,
    ) -> Result<ManifestVersion> {
        // Acquires the write lock of the manifest manager.
        let manager = self.manifest_manager.write().await;
        // Gets current manifest.
        let manifest = manager.manifest();
        // Checks state inside the lock. This is to ensure that we won't update the manifest
        // after `set_readonly_gracefully()` is called.
        let current_state = self.state.load();
        check_state(current_state, manifest.metadata.region_id)?;

        for action in &action_list.actions {
            // Checks whether the edit is still applicable.
            let RegionMetaAction::Edit(edit) = &action else {
                continue;
            };

            edit.validate_shared_reclassification(manifest.metadata.region_id)?;
            // Imports may add shared references, but ordinary edits must not retire
            // or overwrite them. Table publication performs its own locked input check.
            ensure!(
                edit.files_to_remove.iter().all(|file| {
                    file.level != file::LEVEL_SHARED
                        && manifest
                            .files
                            .get(&file.file_id)
                            .is_none_or(|current| current.level != file::LEVEL_SHARED)
                }) && edit.files_to_add.iter().all(|file| {
                    manifest.files.get(&file.file_id).is_none_or(|current| {
                        current.level != file::LEVEL_SHARED || current.as_ref() == file
                    })
                }),
                InvalidRequestSnafu {
                    region_id: manifest.metadata.region_id,
                    reason: "Ordinary manifest edits cannot modify shared SSTs",
                }
            );

            // Checks whether the region is truncated.
            let Some(truncated_entry_id) = manifest.truncated_entry_id else {
                continue;
            };

            // This is an edit from flush.
            if let Some(flushed_entry_id) = edit.flushed_entry_id {
                // A flush edit is valid after truncate in two cases:
                // 1. `flushed_entry_id` moves past `truncated_entry_id`, meaning it definitely
                //    flushed data newer than the truncate point.
                // 2. `flushed_entry_id` equals `truncated_entry_id`, but `flushed_sequence`
                //    increases. This happens in skip-WAL tables where entry id can stay at 0,
                //    while sequence still advances for post-truncate writes.
                //
                // We still reject stale flushes from before truncate:
                // if entry id is equal and sequence does not advance, the flush is outdated.
                let is_newer_entry = truncated_entry_id < flushed_entry_id;
                let is_same_entry_with_newer_sequence = truncated_entry_id == flushed_entry_id
                    && edit.flushed_sequence.is_some_and(|flushed_sequence| {
                        manifest.flushed_sequence < flushed_sequence
                    });

                ensure!(
                    is_newer_entry || is_same_entry_with_newer_sequence,
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

        self.update_and_fire(manager, action_list, is_staging).await
    }

    /// Raises the downgrade start time to now, then marks the region downgrading.
    pub(crate) fn begin_downgrade(&self, region_id: RegionId) {
        let mut fence = self.promotion_fence.lock().unwrap();
        fence.downgrade_started_at = Instant::now();
        self.set_role(RegionRole::DowngradingLeader, region_id);
    }

    /// Promotes the region to a writable leader unless a downgrade started after `issued_at`, a
    /// catchup runs on it, it waits for a reload or it was stopped. `promoted` is false if the
    /// promotion is ignored for one of these reasons; otherwise the role changes as
    /// [`Self::set_role`] allows.
    /// `ended_wait` tells whether this lease ended the region's wait for its Leader lease. A lease
    /// ignored for a downgrade or a catchup ends no wait.
    pub(crate) fn promote_unless_downgrade_started_after(
        &self,
        issued_at: Instant,
        region_id: RegionId,
    ) -> LeaderLeasePromotion {
        // A writable leader that received its Leader lease needs no change and takes no lock. One
        // that awaits it takes the lock, so a catchup either demotes it first or finds the wait
        // ended, see `begin_catchup`; a staging leader is promoted below.
        if self.state.load() == RegionRoleState::Leader(RegionLeaderState::Writable)
            && self.leader_lease_received()
        {
            return LeaderLeasePromotion {
                promoted: true,
                ended_wait: None,
                replaced: None,
            };
        }
        let fence = self.promotion_fence.lock().unwrap();
        if issued_at < fence.downgrade_started_at || fence.catching_up {
            return LeaderLeasePromotion {
                promoted: false,
                ended_wait: None,
                replaced: None,
            };
        }
        let ended_wait = self.record_leader_lease(issued_at);
        let promotion = self.promote_unless_reload_pending(&fence, region_id, false);
        LeaderLeasePromotion {
            promoted: promotion.promoted,
            ended_wait,
            replaced: promotion.replaced,
        }
    }

    /// Promotes a region that caught up to a writable leader and ends its catchup. Returns false
    /// if the region waits for a reload or was stopped.
    pub(crate) fn promote_after_catchup(&self, region_id: RegionId) -> bool {
        let mut fence = self.promotion_fence.lock().unwrap();
        fence.catching_up = false;
        self.promote_unless_reload_pending(&fence, region_id, true)
            .promoted
    }

    /// Returns whether the region was stopped, so no catchup promotes it.
    pub(crate) fn is_stopped(&self) -> bool {
        self.promotion_fence.lock().unwrap().stopped
    }

    /// Begins a catchup unless the region is writable (a writable or staging leader, as
    /// [MitoRegion::is_writable]) and does not [await its Leader lease](Self::awaits_leader_lease),
    /// and returns whether the region awaits its Leader lease, None if it begins none. Until the
    /// catchup ends, Leader leases do not promote the region and end no wait, see
    /// [Self::promote_after_catchup] and [Self::end_catchup]; the caller ends it on every path. A
    /// writable region is demoted to Follower and ignores the Leader leases sent before. A lease
    /// that ends the wait of a writable region holds the same lock.
    pub(crate) fn begin_catchup(&self, region_id: RegionId) -> Option<bool> {
        let mut fence = self.promotion_fence.lock().unwrap();
        let writable = matches!(
            self.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable | RegionLeaderState::Staging)
        );
        let awaits_leader_lease = self.awaits_leader_lease();
        if writable && !awaits_leader_lease {
            return None;
        }
        fence.catching_up = true;
        if writable {
            fence.downgrade_started_at = Instant::now();
            self.set_role(RegionRole::Follower, region_id);
        }
        Some(awaits_leader_lease)
    }

    /// Records a StagingLeader lease as [Self::record_leader_lease] does, unless a catchup runs on
    /// the region.
    pub(crate) fn record_staging_leader_lease(&self, issued_at: Instant) -> Option<Duration> {
        let fence = self.promotion_fence.lock().unwrap();
        if fence.catching_up {
            return None;
        }
        self.record_leader_lease(issued_at)
    }

    /// Demotes the region to a follower for a lease the metasrv granted to it as the candidate of
    /// a migration, unless the region [awaits its Leader lease](Self::awaits_leader_lease) (a
    /// catchup of the migration promoted it, and the route switch that follows grants that lease).
    /// Returns the state this call replaced, None if it left the state unchanged.
    ///
    /// It holds the lock of [Self::promote_after_catchup], so a promotion is either seen or
    /// happens after the demotion.
    pub(crate) fn demote_candidate(&self, region_id: RegionId) -> Option<RegionRoleState> {
        let _fence = self.promotion_fence.lock().unwrap();
        if self.awaits_leader_lease() {
            return None;
        }
        self.set_role(RegionRole::Follower, region_id)
    }

    /// Ends a catchup that does not promote the region, so the next Leader lease promotes it.
    pub(crate) fn end_catchup(&self) {
        self.promotion_fence.lock().unwrap().catching_up = false;
    }

    /// Makes the region wait for its Leader lease as a catchup on a node whose writes need a lease
    /// does, without a catchup: it keeps the memtable and enables the node lease the engine
    /// shares.
    #[cfg(test)]
    pub(crate) fn start_leader_lease_wait_for_test(&self, region_id: RegionId) {
        self.node_lease.enable();
        self.leader_deadline
            .extend(Instant::now() + Duration::from_secs(60));
        assert!(self.promote_after_catchup(region_id));
        assert!(self.awaits_leader_lease());
    }

    /// Marks the region stopped as [MitoRegion::stop] does before it stops the manifest manager.
    #[cfg(test)]
    pub(crate) fn mark_stopped_for_test(&self) {
        self.promotion_fence.lock().unwrap().stopped = true;
    }

    /// Promotes the region unless it waits for a reload or was stopped. The caller holds the fence
    /// lock.
    fn promote_unless_reload_pending(
        &self,
        fence: &PromotionFence,
        region_id: RegionId,
        by_catchup: bool,
    ) -> Promotion {
        if fence.reload_pending || fence.stopped {
            return Promotion {
                promoted: false,
                replaced: None,
            };
        }
        if by_catchup {
            // Before the role, so a lease that finds the region writable compares with it.
            self.catchup_promoted_at
                .store(lease_nanos(Instant::now()).max(1), Ordering::Relaxed);
        }
        Promotion {
            promoted: true,
            replaced: self.set_role(RegionRole::Leader, region_id),
        }
    }

    /// Counts an applied Leader lease (or StagingLeader lease, on a node whose writes need a lease)
    /// of a heartbeat sent at `issued_at` as received since the
    /// last catchup promotion. A heartbeat sent before the promotion does not count: its lease
    /// may have been granted before an earlier downgrade of this node, which
    /// [Self::record_staging_leader_lease] does not check, and the lease of the next heartbeat
    /// counts instead. On a node whose writes need a lease, returns how long the region
    /// [awaited](Self::awaits_leader_lease) its Leader lease if this lease ended the wait: of the
    /// leases that apply together, only the one that ends it returns it.
    pub(crate) fn record_leader_lease(&self, issued_at: Instant) -> Option<Duration> {
        let promoted_at = self.catchup_promoted_at.load(Ordering::Relaxed);
        if promoted_at == 0 || lease_nanos(issued_at) < promoted_at {
            return None;
        }
        self.end_leader_lease_wait(promoted_at)
    }

    /// Ends the wait of the catchup promotion at `promoted_at` if no other lease ended it since
    /// it was read, and returns what [Self::record_leader_lease] returns for the lease that ends it.
    fn end_leader_lease_wait(&self, promoted_at: u64) -> Option<Duration> {
        self.catchup_promoted_at
            .compare_exchange(promoted_at, 0, Ordering::Relaxed, Ordering::Relaxed)
            .ok()?;
        self.node_lease
            .is_enabled()
            .then(|| Duration::from_nanos(lease_nanos(Instant::now()).saturating_sub(promoted_at)))
    }

    /// Returns whether the region is a leader that no longer awaits its Leader lease. It reads both
    /// under the fence lock, under which a catchup demotes and promotes the region and a lease ends
    /// its wait. The changes made without the lock only demote the region, move it between leader
    /// states, skip a leader whose lease came, or promote it directly before ending its wait, so no
    /// read sees an instance a catchup touched as confirmed before a lease confirms it.
    pub(crate) fn is_confirmed_leader(&self) -> bool {
        let _fence = self.promotion_fence.lock().unwrap();
        self.state.load() != RegionRoleState::Follower && !self.awaits_leader_lease()
    }

    /// Returns whether a Leader lease (or a StagingLeader lease, on a node whose writes need a
    /// lease) of a heartbeat sent after the last catchup promotion applied, or no catchup promoted
    /// the region.
    pub(crate) fn leader_lease_received(&self) -> bool {
        self.catchup_promoted_at.load(Ordering::Relaxed) == 0
    }

    /// Returns whether the region, on a node whose writes need a lease, still waits for the
    /// Leader or StagingLeader lease that follows its catchup promotion, whatever its role is
    /// now. Until that lease nothing confirms that it owns the region, even if the route already
    /// points to it, so it commits nothing but replayed tails and what it commits while
    /// downgrading, and starts no work that only such a commit could finish.
    pub(crate) fn awaits_leader_lease(&self) -> bool {
        self.node_lease.is_enabled() && !self.leader_lease_received()
    }

    /// Fails a manifest commit of a region that [awaits its Leader lease](Self::awaits_leader_lease),
    /// unless the region is downgrading: a downgrade is asked only of the leader in the route.
    /// A commit calls it under the manifest lock before it sends anything; a request that would
    /// flush first calls it before it schedules the flush, and an index build before it starts.
    /// It reads atomics only.
    pub(crate) fn ensure_leader_lease_or_downgrading(&self, region_id: RegionId) -> Result<()> {
        ensure!(
            !self.refuses_commits_awaiting_leader_lease(),
            RegionLeaseNotReceivedSnafu { region_id }
        );
        Ok(())
    }

    /// Returns whether [Self::ensure_leader_lease_or_downgrading] fails.
    pub(crate) fn refuses_commits_awaiting_leader_lease(&self) -> bool {
        self.awaits_leader_lease()
            && self.state.load() != RegionRoleState::Leader(RegionLeaderState::Downgrading)
    }

    /// Sets the [`RegionRole`].
    ///
    /// ```text
    ///                  +---------------------+
    ///                  |   Staging Leader    |
    ///                  +----------+----------+
    ///                             |
    ///                             v
    ///     +----------+     +------+-------+     +-------------+
    ///     | Follower | <-> |    Leader    | <-> | Downgrading |
    ///     +-----+----+     +------+-------+     +------+------+
    ///           ^                 ^                    |
    ///           +-----------------+--------------------+
    ///
    /// ```
    ///
    /// # State Transitions
    ///
    /// From `Follower`:
    /// - `Follower -> Leader`
    ///
    /// From `Leader`:
    /// - `Leader -> Follower`
    /// - `Leader -> Downgrading Leader`
    ///
    /// From `Staging Leader`:
    /// - `Staging Leader -> Leader`
    /// - `Staging Leader -> Follower`
    /// - `Staging Leader -> Downgrading Leader`
    ///
    /// From `Downgrading Leader`:
    /// - `Downgrading Leader -> Leader`
    /// - `Downgrading Leader -> Follower`
    ///
    /// A region that is ending staging keeps its state: the end of staging reads the Leader
    /// deadline and the downgrade start when it leaves that state.
    ///
    /// Returns the state this call replaced, None if it left the state unchanged. It comes from
    /// the atomic operation that replaced it.
    pub(crate) fn set_role(
        &self,
        next_role: RegionRole,
        region_id: RegionId,
    ) -> Option<RegionRoleState> {
        match next_role {
            RegionRole::Follower => {
                if self
                    .exit_staging(region_id, RegionRoleState::Follower)
                    .is_ok()
                {
                    info!(
                        "Convert region {} to follower, previous role state: {:?}",
                        region_id,
                        RegionRoleState::Leader(RegionLeaderState::Staging)
                    );
                    return Some(RegionRoleState::Leader(RegionLeaderState::Staging));
                }
                match self.state.fetch_update(|state| {
                    if !matches!(
                        state,
                        RegionRoleState::Follower
                            | RegionRoleState::Leader(RegionLeaderState::EndingStaging)
                    ) {
                        Some(RegionRoleState::Follower)
                    } else {
                        None
                    }
                }) {
                    Ok(state) => {
                        info!(
                            "Convert region {} to follower, previous role state: {:?}",
                            region_id, state
                        );
                        Some(state)
                    }
                    Err(state) => {
                        if state != RegionRoleState::Follower {
                            warn!(
                                "Failed to convert region {} to follower, current role state: {:?}",
                                region_id, state
                            )
                        }
                        None
                    }
                }
            }
            RegionRole::Leader => {
                if self
                    .exit_staging(
                        region_id,
                        RegionRoleState::Leader(RegionLeaderState::Writable),
                    )
                    .is_ok()
                {
                    info!(
                        "Convert region {} to leader, previous role state: {:?}",
                        region_id,
                        RegionRoleState::Leader(RegionLeaderState::Staging)
                    );
                    return Some(RegionRoleState::Leader(RegionLeaderState::Staging));
                }
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
                    Ok(state) => {
                        info!(
                            "Convert region {} to leader, previous role state: {:?}",
                            region_id, state
                        );
                        Some(state)
                    }
                    Err(state) => {
                        // These states already have the leader role; keep their ongoing operation.
                        if !matches!(
                            state,
                            RegionRoleState::Leader(
                                RegionLeaderState::Writable
                                    | RegionLeaderState::Altering
                                    | RegionLeaderState::Dropping
                                    | RegionLeaderState::Truncating
                                    | RegionLeaderState::Editing
                                    | RegionLeaderState::EnteringStaging
                                    | RegionLeaderState::EndingStaging
                            )
                        ) {
                            warn!(
                                "Failed to convert region {} to leader, current role state: {:?}",
                                region_id, state
                            )
                        }
                        None
                    }
                }
            }
            RegionRole::StagingLeader => {
                info!(
                    "Ignore direct conversion of region {} to staging leader; staging requires the dedicated workflow",
                    region_id
                );
                None
            }
            RegionRole::DowngradingLeader => {
                if self
                    .exit_staging(
                        region_id,
                        RegionRoleState::Leader(RegionLeaderState::Downgrading),
                    )
                    .is_ok()
                {
                    info!(
                        "Convert region {} to downgrading region, previous role state: {:?}",
                        region_id,
                        RegionRoleState::Leader(RegionLeaderState::Staging)
                    );
                    return Some(RegionRoleState::Leader(RegionLeaderState::Staging));
                }
                match self.state.compare_exchange(
                    RegionRoleState::Leader(RegionLeaderState::Writable),
                    RegionRoleState::Leader(RegionLeaderState::Downgrading),
                ) {
                    Ok(state) => {
                        info!(
                            "Convert region {} to downgrading region, previous role state: {:?}",
                            region_id, state
                        );
                        Some(state)
                    }
                    Err(state) => {
                        if state != RegionRoleState::Leader(RegionLeaderState::Downgrading) {
                            warn!(
                                "Failed to convert region {} to downgrading leader, current role state: {:?}",
                                region_id, state
                            )
                        }
                        None
                    }
                }
            }
        }
    }

    /// Returns whether the region publishes its manifest by conditional create.
    pub(crate) fn is_conditional_publication(&self) -> bool {
        self.conditional_publication
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

/// The state that decides whether a promotion to Leader applies.
#[derive(Debug)]
struct PromotionFence {
    /// When the region last began a downgrade. A promotion issued before it does not apply.
    ///
    /// It starts at creation time, so promotions issued before the region was opened or reopened
    /// do not apply. A context that replaces this one after a close or reopen is created after
    /// its last downgrade began. A reloaded one is created while this one is still registered, so
    /// it raises the time when it replaces this one.
    downgrade_started_at: Instant,
    /// Whether another writer advanced the manifest, so this instance stays a Follower until a
    /// reload replaces it.
    reload_pending: bool,
    /// Whether a catchup runs on this instance. Until it ends, a Leader lease does not promote
    /// the region: only the catchup's own promotion does.
    catching_up: bool,
    /// Whether the region was stopped (closed, dropped or its worker cleaned up), so no promotion
    /// applies. The manifest manager also knows it, but a promotion decides under this lock only.
    stopped: bool,
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
    /// Returns error if the region does not exist or not flushable.
    pub(crate) fn flushable_region(&self, region_id: RegionId) -> Result<MitoRegionRef> {
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

    /// Remove region by id.
    pub(crate) fn remove_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        let mut regions = self.regions.write().unwrap();
        regions.remove(&region_id)
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
    metric: Option<RegionManifestSizeMetric>,
    pub(crate) total_manifest_size: Arc<AtomicU64>,
    pub(crate) manifest_version: Arc<AtomicU64>,
    pub(crate) file_removed_cnt: Arc<AtomicU64>,
}

impl ManifestStats {
    /// Creates an inactive descriptor; opening a region does not publish a series.
    pub(crate) fn with_region_metrics(region_id: RegionId) -> Self {
        Self {
            metric: Some(RegionManifestSizeMetric::new(region_id)),
            ..Default::default()
        }
    }

    pub(crate) fn metric(&self) -> Option<&RegionManifestSizeMetric> {
        self.metric.as_ref()
    }

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

/// Parses the partition expression from a JSON string.
pub fn parse_partition_expr(partition_expr_str: Option<&str>) -> Result<Option<PartitionExpr>> {
    match partition_expr_str {
        None => Ok(None),
        Some("") => Ok(None),
        Some(json_str) => {
            let expr = partition::expr::PartitionExpr::from_json_str(json_str)
                .with_context(|_| InvalidPartitionExprSnafu { expr: json_str })?;
            Ok(expr)
        }
    }
}

/// Columns requiring allocations or snapshots during an inspection scan.
#[derive(Clone, Copy)]
pub(crate) struct ManifestSstProjection {
    visible: bool,
    table_dir: bool,
    file_id: bool,
    file_path: bool,
    index_file_path: bool,
    partition_expr: bool,
    primary_key_min: bool,
    primary_key_max: bool,
}

impl ManifestSstProjection {
    /// Selects columns by index in [`ManifestSstEntry::schema`]; `None` selects all columns.
    pub(crate) fn new(projection: Option<&[usize]>) -> Self {
        let schema = ManifestSstEntry::schema();
        let included = |name: &str| {
            projection.is_none_or(|indices| {
                indices.iter().any(|index| {
                    schema
                        .column_schemas()
                        .get(*index)
                        .is_some_and(|column| column.name == name)
                })
            })
        };
        Self {
            visible: included(sst_entry::VISIBLE),
            table_dir: included(sst_entry::TABLE_DIR),
            file_id: included(sst_entry::FILE_ID),
            file_path: included(sst_entry::FILE_PATH),
            index_file_path: included(sst_entry::INDEX_FILE_PATH),
            partition_expr: included(sst_entry::PARTITION_EXPR),
            primary_key_min: included(sst_entry::PRIMARY_KEY_MIN),
            primary_key_max: included(sst_entry::PRIMARY_KEY_MAX),
        }
    }
}

/// Applies supported file predicates before allocating an inspection row. Unknown
/// predicates and evaluation failures are left to the query's residual filter.
fn manifest_file_matches(meta: &FileMeta, filters: &MetadataFilter) -> bool {
    filters.matches(|column| {
        Some(match column {
            sst_entry::FILE_SIZE => ScalarValue::UInt64(Some(meta.file_size)),
            sst_entry::LEVEL => ScalarValue::UInt8(Some(meta.level)),
            sst_entry::NUM_ROWS => ScalarValue::UInt64(Some(meta.num_rows)),
            sst_entry::NUM_ROW_GROUPS => ScalarValue::UInt64(Some(meta.num_row_groups)),
            sst_entry::ORIGIN_REGION_ID => ScalarValue::UInt64(Some(meta.region_id.as_u64())),
            sst_entry::MIN_TS => ScalarValue::TimestampNanosecond(
                meta.time_range
                    .0
                    .convert_to(TimeUnit::Nanosecond)
                    .map(|ts| ts.value()),
                None,
            ),
            sst_entry::MAX_TS => ScalarValue::TimestampNanosecond(
                meta.time_range
                    .1
                    .convert_to(TimeUnit::Nanosecond)
                    .map(|ts| ts.value()),
                None,
            ),
            _ => return None,
        })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use common_datasource::compression::CompressionType;
    use common_test_util::temp_dir::create_temp_dir;
    use crossbeam_utils::atomic::AtomicCell;
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use store_api::logstore::provider::Provider;
    use store_api::region_engine::RegionRole;
    use store_api::region_request::{PathType, StagingPartitionDirective};
    use store_api::storage::{FileId, RegionId};

    use crate::access_layer::AccessLayer;
    use crate::error::{Error, ManifestVersionConflictSnafu, Result, UnexpectedSnafu};
    use crate::manifest::action::{
        RegionChange, RegionEdit, RegionMetaAction, RegionMetaActionList, RegionPartitionExprChange,
    };
    use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
    use crate::manifest::storage::delta_file;
    use crate::manifest::storage::staging::staging_manifest_path;
    use crate::region::{
        IndexBuildSource, IndexPublication, IndexPublicationStale, ManifestContext, ManifestStats,
        MitoRegion, PromotionFence, RegionLeaderState, RegionRoleState, RegionStats,
        StagingPartitionInfo, TailPublication, TailsEditor,
    };
    use crate::sst::FormatType;
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::test_util::scheduler_util::SchedulerEnv;
    use crate::test_util::version_util::VersionControlBuilder;
    use crate::time_provider::StdTimeProvider;

    #[test]
    fn test_manifest_file_predicates() {
        use common_recordbatch::filter::MetadataFilter;
        use common_time::Timestamp;
        use datafusion_common::ScalarValue;
        use datafusion_expr::{col, lit};
        let meta = super::FileMeta {
            file_size: 100,
            time_range: (
                Timestamp::new_millisecond(10),
                Timestamp::new_millisecond(20),
            ),
            ..Default::default()
        };
        for (expr, expected) in [
            (col("file_size").lt(lit(101u64)), true),
            (col("file_size").lt(lit(100u64)), false),
            (
                col("min_ts").eq(lit(ScalarValue::TimestampNanosecond(
                    Some(10_000_000),
                    None,
                ))),
                true,
            ),
            (
                col("max_ts").lt(lit(ScalarValue::TimestampNanosecond(
                    Some(20_000_000),
                    None,
                ))),
                false,
            ),
            (col("file_id").eq(lit("unknown")), true),
        ] {
            let filter = MetadataFilter::new(std::slice::from_ref(&expr));
            assert_eq!(
                super::manifest_file_matches(&meta, &filter),
                expected,
                "{expr}"
            );
        }
    }

    #[test]
    fn test_manifest_timestamp_overflow_is_null() {
        use common_recordbatch::filter::MetadataFilter;
        use common_time::Timestamp;
        use datafusion_expr::col;
        let meta = super::FileMeta {
            time_range: (
                Timestamp::new_second(i64::MAX),
                Timestamp::new_second(i64::MAX),
            ),
            ..Default::default()
        };
        let filter = MetadataFilter::new(&[col("min_ts").is_null()]);
        assert!(super::manifest_file_matches(&meta, &filter));
        let filter = MetadataFilter::new(&[col("max_ts").is_not_null()]);
        assert!(!super::manifest_file_matches(&meta, &filter));
    }

    #[test]
    fn test_region_state_lock_free() {
        assert!(AtomicCell::<RegionRoleState>::is_lock_free());
    }

    #[test]
    fn test_region_role_state_as_str() {
        assert_eq!("Follower", RegionRoleState::Follower.as_str());
        assert_eq!(
            "Leader(Writable)",
            RegionRoleState::Leader(RegionLeaderState::Writable).as_str()
        );
        assert_eq!(
            "Leader(Staging)",
            RegionRoleState::Leader(RegionLeaderState::Staging).as_str()
        );
        assert_eq!(
            "Leader(Downgrading)",
            RegionRoleState::Leader(RegionLeaderState::Downgrading).as_str()
        );
    }

    async fn build_test_region(env: &SchedulerEnv) -> MitoRegion {
        build_test_region_with_checkpoint_distance(env, 10).await
    }

    async fn build_test_region_with_checkpoint_distance(
        env: &SchedulerEnv,
        checkpoint_distance: u64,
    ) -> MitoRegion {
        let builder = VersionControlBuilder::new();
        let version_control = Arc::new(builder.build());
        let metadata = version_control.current().version.metadata.clone();

        let manager = RegionManifestManager::new(
            metadata.clone(),
            0,
            RegionManifestOptions {
                file_metadata_interner: Default::default(),
                manifest_dir: "".to_string(),
                object_store: env.access_layer.object_store().clone(),
                compress_type: CompressionType::Uncompressed,
                checkpoint_distance,
                remove_file_options: Default::default(),
                manifest_cache: None,
                conditional_publication: false,
            },
            FormatType::PrimaryKey,
            false,
            &Default::default(),
        )
        .await
        .unwrap();

        let manifest_ctx = Arc::new(ManifestContext::new(
            manager,
            RegionRoleState::Leader(RegionLeaderState::Writable),
            None,
            Default::default(),
        ));

        MitoRegion {
            region_id: metadata.region_id,
            compaction_state: Default::default(),
            version_control,
            access_layer: env.access_layer.clone(),
            manifest_ctx,
            file_purger: crate::test_util::new_noop_file_purger(),
            provider: Provider::noop_provider(),
            last_flush_millis: Default::default(),
            last_schedule_compaction_millis: Default::default(),
            time_provider: Arc::new(StdTimeProvider),
            topic_latest_entry_id: Default::default(),
            region_stats: RegionStats::new(),
            stats: ManifestStats::default(),
        }
    }

    #[tokio::test]
    async fn test_manifest_stream_does_not_pin_sst_handles() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        use common_recordbatch::filter::MetadataFilter;
        use datafusion_expr::{col, lit};
        use futures::StreamExt;

        use super::ManifestSstProjection;
        use crate::sst::file::FileMeta;
        use crate::sst::file_purger::FilePurger;

        #[derive(Debug, Default)]
        struct CountingPurger(AtomicUsize);

        impl FilePurger for CountingPurger {
            fn purge_file(&self, _: FileMeta) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        for include_visible in [false, true] {
            let env = SchedulerEnv::new().await;
            let region = build_test_region(&env).await;
            let purger = Arc::new(CountingPurger::default());
            let files = (0..2)
                .map(|_| FileMeta {
                    region_id: region.region_id,
                    file_id: FileId::random(),
                    ..Default::default()
                })
                .collect::<Vec<_>>();
            let mut edit = empty_edit();
            edit.files_to_add = files.clone();
            region
                .manifest_ctx
                .manifest_manager
                .write()
                .await
                .update(
                    RegionMetaActionList::with_action(RegionMetaAction::Edit(edit.clone())),
                    false,
                )
                .await
                .unwrap();
            region
                .version_control
                .apply_edit(Some(edit), &[], purger.clone());

            let schema = store_api::sst_entry::ManifestSstEntry::schema();
            let columns = if include_visible {
                vec!["file_id", "visible"]
            } else {
                vec!["file_id"]
            };
            let projection = columns
                .iter()
                .map(|name| {
                    schema
                        .column_schemas()
                        .iter()
                        .position(|column| column.name == *name)
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let projection = ManifestSstProjection::new(Some(&projection));
            let snapshot = region.manifest_visible_file_ids(projection);
            assert_eq!(snapshot.is_some(), include_visible);
            if let Some(snapshot) = snapshot {
                assert_eq!(snapshot, files.iter().map(|file| file.file_id).collect());
            }
            // A direct caller may omit a residual column. The source must not
            // interpret an unmaterialized visible value as grounds for pruning.
            let filters = MetadataFilter::new(&[col("visible").eq(lit(true))]);
            let mut stream = region.manifest_sst_stream(filters, projection);
            let first = stream.next().await.unwrap();
            if include_visible {
                assert!(first.visible);
            }
            let mut removal = empty_edit();
            removal.files_to_remove = files;
            region
                .version_control
                .apply_edit(Some(removal), &[], purger.clone());
            // Both final FileHandles must reach the purger while the stream is
            // suspended at its first yield, rather than waiting for cancellation.
            assert_eq!(purger.0.load(Ordering::SeqCst), 2);
            let second = stream.next().await.unwrap();
            if include_visible {
                assert!(
                    second.visible,
                    "visibility must retain the original snapshot"
                );
            }
            assert!(stream.next().await.is_none());
        }
    }

    #[tokio::test]
    async fn test_manifest_narthex_file_path_filter() {
        use common_recordbatch::filter::MetadataFilter;
        use datafusion_expr::{col, lit};
        use futures::StreamExt;

        use super::ManifestSstProjection;
        use crate::sst::file::{FileFormat, FileMeta, NarthexPartMeta, NarthexSamplesMeta};

        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let id = FileId::random();
        let path = format!(
            "{}manifest.json",
            crate::sst::location::narthex_file_dir(
                &region.access_layer.build_region_dir(region.region_id),
                id
            )
        );
        let mut edit = empty_edit();
        edit.files_to_add.push(FileMeta {
            file_id: id,
            region_id: region.region_id,
            file_size: 2,
            format: FileFormat::NarthexSamples(Box::new(NarthexSamplesMeta {
                manifest_size: 1,
                parts: smallvec::smallvec![NarthexPartMeta { file_size: 1 }],
            })),
            ..Default::default()
        });
        region
            .manifest_ctx
            .manifest_manager
            .write()
            .await
            .update(
                RegionMetaActionList::with_action(RegionMetaAction::Edit(edit)),
                false,
            )
            .await
            .unwrap();

        for filters in [
            MetadataFilter::new(&[]),
            MetadataFilter::new(&[col("file_path").eq(lit(path.as_str()))]),
        ] {
            let mut stream = region.manifest_sst_stream(filters, ManifestSstProjection::new(None));
            assert_eq!(stream.next().await.unwrap().file_path, path);
            assert!(stream.next().await.is_none());
        }
    }

    fn empty_edit() -> RegionEdit {
        RegionEdit {
            series_indexes_to_add: Vec::new(),
            series_indexes_to_remove: Vec::new(),
            files_to_add: Vec::new(),
            files_to_remove: Vec::new(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_promoting_writable_leader_takes_no_lock() {
        let env = SchedulerEnv::new().await;
        let region = Arc::new(build_test_region(&env).await);
        let _promotion_fence = region.manifest_ctx.promotion_fence.lock().unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let promoted = region.clone();
        std::thread::spawn(move || {
            let manifest_ctx = &promoted.manifest_ctx;
            let issued_at = std::time::Instant::now();
            tx.send(
                manifest_ctx
                    .promote_unless_downgrade_started_after(issued_at, promoted.region_id)
                    .promoted,
            )
            .unwrap();
        });
        assert!(rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap());
    }

    /// Makes the test region a writable leader that awaits its Leader lease.
    fn await_leader_lease(region: &MitoRegion) {
        let manifest_ctx = &region.manifest_ctx;
        manifest_ctx.node_lease.enable();
        manifest_ctx
            .leader_deadline
            .extend(std::time::Instant::now() + std::time::Duration::from_secs(60));
        region.set_role(RegionRole::Follower);
        assert!(manifest_ctx.promote_after_catchup(region.region_id));
        assert!(region.is_writable());
        assert!(manifest_ctx.awaits_leader_lease());
    }

    /// Runs `f` on another thread while this thread holds the fence lock, checks that `f` waits
    /// for the lock, then runs `under_lock` before it releases the lock and returns what `f` returns.
    fn run_blocked_on_fence<T: Send + 'static>(
        region: &Arc<MitoRegion>,
        f: impl FnOnce(&MitoRegion) -> T + Send + 'static,
        under_lock: impl FnOnce(&mut PromotionFence),
    ) -> T {
        let mut fence = region.manifest_ctx.promotion_fence.lock().unwrap();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (tx, rx) = std::sync::mpsc::channel();
        let blocked = region.clone();
        std::thread::spawn(move || {
            ready_tx.send(()).unwrap();
            tx.send(f(&blocked)).unwrap();
        });
        ready_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
        assert!(
            rx.recv_timeout(std::time::Duration::from_millis(200))
                .is_err()
        );
        under_lock(&mut fence);
        drop(fence);
        rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()
    }

    /// A Leader lease of a writable leader that awaits it takes the fence lock, so it cannot end
    /// the wait between a catchup's flag and its demotion.
    #[tokio::test]
    async fn test_lease_awaited_by_writable_leader_waits_for_catchup_demotion() {
        let env = SchedulerEnv::new().await;
        let region = Arc::new(build_test_region(&env).await);
        await_leader_lease(&region);
        let issued_at = std::time::Instant::now();
        let promotion = run_blocked_on_fence(
            &region,
            move |r| {
                r.manifest_ctx
                    .promote_unless_downgrade_started_after(issued_at, r.region_id)
            },
            // What a catchup does under the lock.
            |fence| {
                fence.catching_up = true;
                region
                    .manifest_ctx
                    .set_role(RegionRole::Follower, region.region_id);
            },
        );
        assert!(!promotion.promoted);
        assert!(promotion.ended_wait.is_none());
        assert!(region.manifest_ctx.awaits_leader_lease());
    }

    /// A candidate lease reads the wait for a Leader lease under the fence lock, so it does not
    /// demote a region that a catchup promotes meanwhile.
    #[tokio::test]
    async fn test_candidate_demotion_sees_catchup_promotion_under_fence() {
        let env = SchedulerEnv::new().await;
        let region = Arc::new(build_test_region(&env).await);
        region.manifest_ctx.node_lease.enable();
        // A writable leader, so a demotion that misses the fence returns Some and fails below.
        assert!(region.is_writable());
        assert!(!region.manifest_ctx.awaits_leader_lease());
        let demoted = run_blocked_on_fence(
            &region,
            |r| r.manifest_ctx.demote_candidate(r.region_id),
            // What a catchup's promotion does under the lock.
            |fence| {
                assert!(
                    region
                        .manifest_ctx
                        .promote_unless_reload_pending(fence, region.region_id, true)
                        .promoted
                )
            },
        );
        assert_eq!(demoted, None);
        assert!(region.manifest_ctx.awaits_leader_lease());
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
    }

    /// The readiness of an upgrade reads the role and the wait for a Leader lease under the fence
    /// lock, so it sees a change of both made under that lock as a whole.
    #[tokio::test]
    async fn test_confirmed_leader_read_waits_for_fence_lock() {
        let env = SchedulerEnv::new().await;
        let region = Arc::new(build_test_region(&env).await);
        await_leader_lease(&region);
        // A lease ends the wait under the lock; a read made before it would answer false.
        let confirmed = run_blocked_on_fence(
            &region,
            |r| r.is_confirmed_leader(),
            |_| {
                region
                    .manifest_ctx
                    .catchup_promoted_at
                    .store(0, Ordering::Relaxed)
            },
        );
        assert!(confirmed);
    }

    /// A catchup begins on no writable leader whose wait for a Leader lease ended, and promotes no
    /// stopped region.
    #[tokio::test]
    async fn test_catchup_skips_confirmed_leader_and_promotes_no_stopped_region() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        await_leader_lease(&region);
        let promotion = region
            .manifest_ctx
            .promote_unless_downgrade_started_after(std::time::Instant::now(), region.region_id);
        assert!(promotion.ended_wait.is_some());
        assert!(!region.manifest_ctx.awaits_leader_lease());
        assert_eq!(region.manifest_ctx.begin_catchup(region.region_id), None);
        assert!(region.is_writable());
        region.set_role(RegionRole::Follower);
        region.stop().await;
        assert!(region.manifest_ctx.is_stopped());
        assert!(!region.manifest_ctx.promote_after_catchup(region.region_id));
        assert!(region.is_follower());
    }

    #[tokio::test]
    async fn test_awaiting_leader_lease_requires_verification() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let manifest_ctx = &region.manifest_ctx;
        manifest_ctx.node_lease.enable();
        manifest_ctx
            .leader_deadline
            .extend(std::time::Instant::now() + std::time::Duration::from_secs(60));
        assert!(!manifest_ctx.must_verify_commit());

        region.set_role(RegionRole::Follower);
        assert!(manifest_ctx.promote_after_catchup(region.region_id));
        assert!(manifest_ctx.must_verify_commit());

        std::thread::sleep(std::time::Duration::from_millis(1));
        manifest_ctx.record_leader_lease(std::time::Instant::now());
        assert!(!manifest_ctx.must_verify_commit());
    }

    #[tokio::test]
    async fn test_lease_sent_before_catchup_promotion_does_not_count() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let manifest_ctx = &region.manifest_ctx;
        let sent_before = std::time::Instant::now();
        region.set_role(RegionRole::Follower);
        std::thread::sleep(std::time::Duration::from_millis(1));
        assert!(manifest_ctx.promote_after_catchup(region.region_id));

        assert!(
            manifest_ctx
                .promote_unless_downgrade_started_after(sent_before, region.region_id)
                .promoted
        );
        assert!(!manifest_ctx.leader_lease_received());
        let sent_after = std::time::Instant::now();
        assert!(
            manifest_ctx
                .promote_unless_downgrade_started_after(sent_after, region.region_id)
                .promoted
        );
        assert!(manifest_ctx.leader_lease_received());
    }

    /// A replay of pending tails that commits while the region awaits its Leader lease starts no
    /// checkpoint without conditional publication too, where no verification of the commit
    /// turns it off.
    #[tokio::test]
    async fn test_tail_commit_awaiting_leader_lease_starts_no_checkpoint_without_conditional_publication()
     {
        let env = SchedulerEnv::new().await;
        let region = build_test_region_with_checkpoint_distance(&env, 1).await;
        let manifest_ctx = &region.manifest_ctx;
        manifest_ctx.start_leader_lease_wait_for_test(region.region_id);
        let action_list =
            RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit::default()));

        let publication = manifest_ctx
            .update_manifest_for_tails(&[], action_list, TailsEditor::Replay)
            .await
            .unwrap();

        assert!(matches!(publication, TailPublication::Committed(_)));
        let manager = manifest_ctx.manifest_manager.read().await;
        assert_eq!(0, manager.checkpointer().last_checkpoint_version());
        assert!(!manager.checkpointer().is_doing_checkpoint());
    }

    /// Of the leases that apply together, only the one that ends the wait reports it.
    #[tokio::test]
    async fn test_only_the_lease_that_ends_the_wait_reports_it() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let manifest_ctx = &region.manifest_ctx;
        manifest_ctx.start_leader_lease_wait_for_test(region.region_id);
        // Two leases that read the same wait before either ends it.
        let promoted_at = manifest_ctx.catchup_promoted_at.load(Ordering::Relaxed);

        assert!(manifest_ctx.end_leader_lease_wait(promoted_at).is_some());
        assert!(manifest_ctx.end_leader_lease_wait(promoted_at).is_none());
        assert!(!manifest_ctx.awaits_leader_lease());
        // A lease that reads the wait after it ended.
        assert!(
            manifest_ctx
                .record_leader_lease(std::time::Instant::now())
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_compaction_update_manifest_allows_editing_state() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        region.set_editing(RegionLeaderState::Writable).unwrap();

        let file_id = FileId::random();
        let action_list = RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
            files_to_add: vec![crate::sst::file::FileMeta {
                region_id: region.region_id,
                file_id,
                level: 1,
                ..Default::default()
            }],
            files_to_remove: Vec::new(),
            timestamp_ms: None,
            compaction_time_window: None,
            flushed_entry_id: None,
            flushed_sequence: None,
            committed_sequence: None,
            ..Default::default()
        }));

        region
            .manifest_ctx
            .update_manifest_for_compaction(action_list)
            .await
            .unwrap();

        assert!(
            region
                .manifest_ctx
                .manifest()
                .await
                .files
                .contains_key(&file_id)
        );
    }

    #[rstest::rstest]
    #[case::writable(false)]
    #[case::downgrading(true)]
    #[tokio::test]
    async fn test_index_publication_refused_awaiting_leader_lease_unless_downgrading(
        #[case] downgrading: bool,
    ) {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let source = crate::sst::file::FileMeta {
            region_id: region.region_id,
            file_id: FileId::random(),
            level: 1,
            file_size: 1024,
            ..Default::default()
        };
        region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                    files_to_add: vec![source.clone()],
                    ..empty_edit()
                })),
                false,
            )
            .await
            .unwrap();
        let manifest_ctx = &region.manifest_ctx;
        manifest_ctx.start_leader_lease_wait_for_test(region.region_id);
        let index_source =
            IndexBuildSource::new(source.clone(), region.version().metadata.schema_version);
        let mut updated = source.clone();
        updated.index_version = 1;
        updated.index_file_size = 128;
        let publish = || manifest_ctx.update_manifest_for_index(&index_source, updated.clone());

        if downgrading {
            // A downgrade is asked only of the leader in the route.
            manifest_ctx.begin_downgrade(region.region_id);
        } else {
            let err = publish().await.unwrap_err();
            assert!(
                matches!(err, Error::RegionLeaseNotReceived { .. }),
                "{err:?}"
            );
            assert_eq!(
                manifest_ctx
                    .manifest()
                    .await
                    .files
                    .get(&source.file_id)
                    .map(AsRef::as_ref),
                Some(&source)
            );
            manifest_ctx.record_leader_lease(std::time::Instant::now());
        }
        assert!(matches!(
            publish().await.unwrap(),
            IndexPublication::Committed { .. }
        ));
    }

    #[tokio::test]
    async fn test_index_publication_rejects_older_source_generation() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let source = crate::sst::file::FileMeta {
            region_id: region.region_id,
            file_id: FileId::random(),
            level: 1,
            file_size: 1024,
            ..Default::default()
        };
        region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                    files_to_add: vec![source.clone()],
                    ..empty_edit()
                })),
                false,
            )
            .await
            .unwrap();

        let mut first_update = source.clone();
        first_update.index_version = 1;
        first_update.index_file_size = 128;
        let schema_version = region.version().metadata.schema_version;
        let initial_source = IndexBuildSource::new(source.clone(), schema_version);
        let first_committed = match region
            .manifest_ctx
            .update_manifest_for_index(&initial_source, first_update)
            .await
            .unwrap()
        {
            IndexPublication::Committed { file_meta, .. } => file_meta,
            IndexPublication::Stale(_) => panic!("first index publication should commit"),
        };

        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let delayed_manifest_ctx = region.manifest_ctx.clone();
        let delayed_source = IndexBuildSource::new(first_committed.clone(), schema_version);
        let delayed_publication = tokio::spawn(async move {
            let mut delayed_update = delayed_source.file_meta.clone();
            delayed_update.index_version = 2;
            delayed_update.index_file_size = 128;
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            delayed_manifest_ctx
                .update_manifest_for_index(&delayed_source, delayed_update)
                .await
        });
        ready_rx.await.unwrap();

        let mut newer_update = first_committed.clone();
        newer_update.index_version = 3;
        newer_update.index_file_size = 256;
        let newer_source = IndexBuildSource::new(first_committed, schema_version);
        let newer_committed = match region
            .manifest_ctx
            .update_manifest_for_index(&newer_source, newer_update)
            .await
            .unwrap()
        {
            IndexPublication::Committed { file_meta, .. } => file_meta,
            IndexPublication::Stale(_) => panic!("newer index publication should commit"),
        };

        release_tx.send(()).unwrap();
        assert!(matches!(
            delayed_publication.await.unwrap().unwrap(),
            IndexPublication::Stale(IndexPublicationStale::SourceChanged)
        ));
        assert_eq!(
            region
                .manifest_ctx
                .manifest()
                .await
                .files
                .get(&source.file_id)
                .map(AsRef::as_ref),
            Some(&newer_committed)
        );
    }

    #[tokio::test]
    async fn test_index_publication_rejects_older_schema_generation() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;
        let source_meta = crate::sst::file::FileMeta {
            region_id: region.region_id,
            file_id: FileId::random(),
            level: 1,
            file_size: 1024,
            ..Default::default()
        };
        region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Edit(RegionEdit {
                    files_to_add: vec![source_meta.clone()],
                    ..empty_edit()
                })),
                false,
            )
            .await
            .unwrap();

        let old_schema_version = region.version().metadata.schema_version;
        let source = IndexBuildSource::new(source_meta.clone(), old_schema_version);
        let mut new_metadata = region.version().metadata.as_ref().clone();
        new_metadata.schema_version += 1;
        region
            .manifest_ctx
            .update_manifest(
                RegionLeaderState::Writable,
                RegionMetaActionList::with_action(RegionMetaAction::Change(RegionChange {
                    metadata: Arc::new(new_metadata),
                    sst_format: FormatType::PrimaryKey,
                    append_mode: None,
                })),
                false,
            )
            .await
            .unwrap();

        let mut updated = source_meta.clone();
        updated.index_version = 1;
        updated.index_file_size = 128;
        assert!(matches!(
            region
                .manifest_ctx
                .update_manifest_for_index(&source, updated)
                .await
                .unwrap(),
            IndexPublication::Stale(IndexPublicationStale::SchemaChanged)
        ));

        let manifest = region.manifest_ctx.manifest().await;
        assert_eq!(manifest.metadata.schema_version, old_schema_version + 1);
        assert_eq!(
            manifest.files.get(&source_meta.file_id).map(AsRef::as_ref),
            Some(&source_meta)
        );
    }

    #[tokio::test]
    async fn test_end_staging_partition_expr_change_and_edit_success() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;

        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();
        let version = manager.last_version();
        for action in [
            RegionMetaAction::PartitionExprChange(RegionPartitionExprChange {
                partition_expr: Some("expr_a".to_string()),
            }),
            RegionMetaAction::Edit(empty_edit()),
        ] {
            manager
                .update(RegionMetaActionList::with_action(action), true)
                .await
                .unwrap();
        }
        // Staged actions take no version.
        assert_eq!(manager.last_version(), version);

        let pending = region.end_staging_locked(&mut manager).await.unwrap();
        assert_eq!(pending.unwrap().version(), version + 1);
        assert_eq!(manager.last_version(), version + 1);
        assert!(manager.staged_actions().is_none());
        drop(manager);

        assert_eq!(
            region.version().metadata.partition_expr.as_deref(),
            Some("expr_a")
        );
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
    }

    #[tokio::test]
    async fn test_end_staging_change_with_same_columns_success() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;

        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();

        let mut changed_metadata = region.version().metadata.as_ref().clone();
        changed_metadata.set_partition_expr(Some("expr_b".to_string()));

        manager
            .update(
                RegionMetaActionList::new(vec![
                    RegionMetaAction::Change(RegionChange {
                        metadata: Arc::new(changed_metadata),
                        sst_format: FormatType::PrimaryKey,
                        append_mode: None,
                    }),
                    RegionMetaAction::Edit(empty_edit()),
                ]),
                true,
            )
            .await
            .unwrap();

        let _hook_payload = region.end_staging_locked(&mut manager).await.unwrap();
        drop(manager);

        assert_eq!(
            region.version().metadata.partition_expr.as_deref(),
            Some("expr_b")
        );
        assert_eq!(
            region.state(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
    }

    #[tokio::test]
    async fn test_end_staging_change_with_different_columns_fails() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;

        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();

        let mut changed_metadata = region.version().metadata.as_ref().clone();
        changed_metadata.column_metadatas.rotate_left(1);

        manager
            .update(
                RegionMetaActionList::new(vec![
                    RegionMetaAction::Change(RegionChange {
                        metadata: Arc::new(changed_metadata),
                        sst_format: FormatType::PrimaryKey,
                        append_mode: None,
                    }),
                    RegionMetaAction::Edit(empty_edit()),
                ]),
                true,
            )
            .await
            .unwrap();

        let result = region.end_staging_locked(&mut manager).await;
        assert!(matches!(result, Err(Error::Unexpected { .. })));
    }

    #[tokio::test]
    async fn test_end_staging_partition_expr_change_and_change_conflict_fails() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;

        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();

        let mut changed_metadata = region.version().metadata.as_ref().clone();
        changed_metadata.set_partition_expr(Some("expr_c".to_string()));

        manager
            .update(
                RegionMetaActionList::new(vec![
                    RegionMetaAction::PartitionExprChange(RegionPartitionExprChange {
                        partition_expr: Some("expr_c".to_string()),
                    }),
                    RegionMetaAction::Change(RegionChange {
                        metadata: Arc::new(changed_metadata),
                        sst_format: FormatType::PrimaryKey,
                        append_mode: None,
                    }),
                    RegionMetaAction::Edit(empty_edit()),
                ]),
                true,
            )
            .await
            .unwrap();

        let result = region.end_staging_locked(&mut manager).await;
        assert!(matches!(result, Err(Error::Unexpected { .. })));
    }

    #[tokio::test]
    async fn test_end_staging_ignores_files_in_staging_dir() {
        let env = SchedulerEnv::new().await;
        let region = build_test_region(&env).await;

        let mut manager = region.manifest_ctx.manifest_manager.write().await;
        region.set_staging(&mut manager).await.unwrap();
        // A late writer leaves a delta in the staging directory.
        let late_file = crate::sst::file::FileMeta {
            file_id: FileId::random(),
            ..Default::default()
        };
        let late_edit = RegionEdit {
            files_to_add: vec![late_file.clone()],
            ..Default::default()
        };
        region
            .access_layer
            .object_store()
            .write(
                &format!(
                    "{}{}",
                    staging_manifest_path(manager.store().manifest_dir()),
                    delta_file(1)
                ),
                RegionMetaActionList::with_action(RegionMetaAction::Edit(late_edit))
                    .encode()
                    .unwrap(),
            )
            .await
            .unwrap();
        manager
            .update(
                RegionMetaActionList::new(vec![
                    RegionMetaAction::PartitionExprChange(RegionPartitionExprChange {
                        partition_expr: Some("expr_a".to_string()),
                    }),
                    RegionMetaAction::Edit(empty_edit()),
                ]),
                true,
            )
            .await
            .unwrap();

        let _hook_payload = region.end_staging_locked(&mut manager).await.unwrap();

        assert!(!manager.manifest().files.contains_key(&late_file.file_id));
    }

    #[tokio::test]
    async fn test_ending_staging_holds_role_changes() {
        let env = SchedulerEnv::new().await;
        let version_control = VersionControlBuilder::new().build();
        let ctx = env
            .mock_manifest_context(version_control.current().version.metadata.clone())
            .await;
        let region_id = RegionId::new(1024, 0);
        let staging = RegionRoleState::Leader(RegionLeaderState::Staging);
        let committed: Result<()> = Ok(());
        let unknown: Result<()> = UnexpectedSnafu { reason: "mock" }.fail();
        let conflict: Result<()> = ManifestVersionConflictSnafu {
            version: 1u64,
            manifest_dir: "manifest/",
            reason: "mock",
        }
        .fail();
        // Ends staging with `result`, running `during` while the commit runs.
        let end = |result: &Result<()>, during: &dyn Fn()| {
            ctx.state.store(staging);
            if ctx.staging_partition_info().is_none() {
                ctx.set_staging_partition_info(StagingPartitionInfo::from_partition_directive(
                    StagingPartitionDirective::RejectAllWrites,
                ));
            }
            let started = ctx.begin_ending_staging(region_id).unwrap();
            during();
            assert_eq!(
                ctx.state.load(),
                RegionRoleState::Leader(RegionLeaderState::EndingStaging)
            );
            let state = ctx.leave_ending_staging(region_id, started, result);
            // Only a return to staging keeps the partition info for the retry.
            assert_eq!(ctx.staging_partition_info().is_some(), state == staging);
            state
        };
        let set_roles = || {
            for role in [
                RegionRole::Follower,
                RegionRole::Leader,
                RegionRole::StagingLeader,
                RegionRole::DowngradingLeader,
            ] {
                ctx.set_role(role, region_id);
            }
        };

        assert_eq!(end(&unknown, &set_roles), staging);
        assert_eq!(
            end(&committed, &|| ctx.begin_downgrade(region_id)),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        );
        assert_eq!(
            end(&committed, &|| {}),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );
        assert_eq!(end(&conflict, &|| {}), RegionRoleState::Follower);
        // The Leader lease expired while the commit ran; no deadline counts as expired.
        ctx.node_lease.enable();
        assert_eq!(end(&committed, &|| {}), RegionRoleState::Follower);
        assert_eq!(end(&unknown, &|| {}), RegionRoleState::Follower);
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
        // Each call returns the state it replaced, None if it left the state unchanged.
        // Leader -> Follower
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Follower, region_id),
            Some(RegionRoleState::Leader(RegionLeaderState::Writable))
        );
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Follower -> Follower
        assert_eq!(manifest_ctx.set_role(RegionRole::Follower, region_id), None);

        // Follower -> Leader
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Leader, region_id),
            Some(RegionRoleState::Follower)
        );
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );

        // Direct Leader -> StagingLeader should be ignored.
        assert_eq!(
            manifest_ctx.set_role(RegionRole::StagingLeader, region_id),
            None
        );
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );

        // Leader -> Downgrading Leader
        assert_eq!(
            manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id),
            Some(RegionRoleState::Leader(RegionLeaderState::Writable))
        );
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        );

        // Downgrading Leader -> Follower
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Follower, region_id),
            Some(RegionRoleState::Leader(RegionLeaderState::Downgrading))
        );
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Can't downgrade from follower (Follower -> Downgrading Leader)
        assert_eq!(
            manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id),
            None
        );
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Set region role too Downgrading Leader
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Leader, region_id),
            Some(RegionRoleState::Follower)
        );
        assert_eq!(
            manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id),
            Some(RegionRoleState::Leader(RegionLeaderState::Writable))
        );
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Downgrading)
        );

        assert_eq!(
            manifest_ctx.set_role(RegionRole::DowngradingLeader, region_id),
            None
        );

        // Downgrading Leader -> Leader
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Leader, region_id),
            Some(RegionRoleState::Leader(RegionLeaderState::Downgrading))
        );
        assert_eq!(
            manifest_ctx.state.load(),
            RegionRoleState::Leader(RegionLeaderState::Writable)
        );

        assert_eq!(manifest_ctx.set_role(RegionRole::Leader, region_id), None);

        // Altering Leader -> Follower
        let altering = RegionRoleState::Leader(RegionLeaderState::Altering);
        manifest_ctx.state.store(altering);
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Follower, region_id),
            Some(altering)
        );
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        // Staging Leader -> Follower
        let staging = RegionRoleState::Leader(RegionLeaderState::Staging);
        manifest_ctx.state.store(staging);
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Follower, region_id),
            Some(staging)
        );
        assert_eq!(manifest_ctx.state.load(), RegionRoleState::Follower);

        manifest_ctx.state.store(staging);
        assert_eq!(
            manifest_ctx.set_role(RegionRole::Leader, region_id),
            Some(staging)
        );
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
                    file_metadata_interner: Default::default(),
                    manifest_dir: "".to_string(),
                    object_store: env.access_layer.object_store().clone(),
                    compress_type: CompressionType::Uncompressed,
                    checkpoint_distance: 10,
                    remove_file_options: Default::default(),
                    manifest_cache: None,
                    conditional_publication: false,
                },
                FormatType::PrimaryKey,
                false,
                &Default::default(),
            )
            .await
            .unwrap();
            Arc::new(ManifestContext::new(
                manager,
                RegionRoleState::Leader(RegionLeaderState::Staging),
                None,
                Default::default(),
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
        let object_store = ObjectStore::new(fs_builder).unwrap();

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
                file_metadata_interner: Default::default(),
                manifest_dir: "".to_string(),
                object_store: access_layer.object_store().clone(),
                compress_type: CompressionType::Uncompressed,
                checkpoint_distance: 10,
                remove_file_options: Default::default(),
                manifest_cache: None,
                conditional_publication: false,
            },
            FormatType::PrimaryKey,
            false,
            &Default::default(),
        )
        .await
        .unwrap();

        let manifest_ctx = Arc::new(ManifestContext::new(
            manager,
            RegionRoleState::Leader(RegionLeaderState::Writable),
            None,
            Default::default(),
        ));

        let region = MitoRegion {
            region_id: metadata.region_id,
            compaction_state: Default::default(),
            version_control,
            access_layer,
            manifest_ctx: manifest_ctx.clone(),
            file_purger: crate::test_util::new_noop_file_purger(),
            provider: Provider::noop_provider(),
            last_flush_millis: Default::default(),
            last_schedule_compaction_millis: Default::default(),
            time_provider: Arc::new(StdTimeProvider),
            topic_latest_entry_id: Default::default(),
            region_stats: RegionStats::new(),
            stats: ManifestStats::default(),
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

        // Entering staging clears the staging directory and the staged actions.
        {
            let mut manager = manifest_ctx.manifest_manager.write().await;
            let object_store = region.access_layer.object_store().clone();
            // A delta that an older version staged.
            let stale_delta = format!(
                "{}{}",
                staging_manifest_path(manager.store().manifest_dir()),
                delta_file(100)
            );
            object_store
                .write(
                    &stale_delta,
                    RegionMetaActionList::new(vec![]).encode().unwrap(),
                )
                .await
                .unwrap();
            region.set_staging(&mut manager).await.unwrap();
            assert!(!object_store.exists(&stale_delta).await.unwrap());

            manager
                .update(
                    RegionMetaActionList::with_action(RegionMetaAction::Edit(empty_edit())),
                    true,
                )
                .await
                .unwrap();
            assert!(manager.staged_actions().is_some());
            region.exit_staging().unwrap();
            region.set_staging(&mut manager).await.unwrap();
            assert!(manager.staged_actions().is_none());
            assert!(manager.staging_manifest().is_none());

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
