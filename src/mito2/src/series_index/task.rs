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

//! Worker-owned background maintenance for series indexes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use object_store::ObjectStore;
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::metrics::{
    SERIES_INDEX_CAPACITY_DEFERRED, SERIES_INDEX_DISK_BYTES, SERIES_INDEX_RECONCILE_TOTAL,
};
use crate::region::{RegionLeaderState, RegionMapRef, RegionRoleState};
use crate::series_index::maintenance::reconcile_series_indexes;
use crate::series_index::purger::{IndexFilePurger, PurgeRequest, run_index_purge_task};
use crate::time_provider::TimeProviderRef;

/// Shared lifecycle state for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    running: AtomicBool,
    notify: Notify,
}

impl SeriesIndexTaskState {
    pub(crate) fn new() -> Self {
        Self {
            running: AtomicBool::new(true),
            notify: Notify::new(),
        }
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        self.notify.notify_one();
    }

    pub(crate) fn stop(&self) {
        self.running.store(false, Ordering::Release);
        // Retain a permit if maintenance has not started waiting yet.
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Starts both tasks on the compaction runtime, detaching purge and returning the maintenance handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    bucket_width: Duration,
    purger: IndexFilePurger,
    purge_receiver: UnboundedReceiver<PurgeRequest>,
    disk_usage: Arc<AtomicU64>,
    max_size: u64,
    interval: Duration,
    time_provider: TimeProviderRef,
    enable_range_index: bool,
) -> JoinHandle<()> {
    // Snapshots may retain senders after the worker stops; purge until all senders drop.
    common_runtime::spawn_compact(run_index_purge_task(
        worker_id,
        store.clone(),
        purge_receiver,
    ));
    common_runtime::spawn_compact(async move {
        SeriesIndexTask {
            worker_id,
            store,
            regions,
            bucket_width,
            purger,
            disk_usage,
            max_size,
            reported_usage: 0,
            state,
            interval,
            time_provider,
            enable_range_index,
        }
        .run()
        .await;
    })
}

/// Periodic series-index maintenance for one region worker.
struct SeriesIndexTask {
    store: ObjectStore,
    regions: RegionMapRef,
    bucket_width: Duration,
    purger: IndexFilePurger,
    disk_usage: Arc<AtomicU64>,
    max_size: u64,
    reported_usage: u64,
    worker_id: u32,
    state: Arc<SeriesIndexTaskState>,
    interval: Duration,
    time_provider: TimeProviderRef,
    enable_range_index: bool,
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.time_provider.wait_duration(self.interval);
        let mut timer = tokio::time::interval_at(Instant::now() + interval, interval);
        // Schedule future ticks from a late tick rather than the original cadence.
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        while self.state.is_running() {
            tokio::select! {
                _ = self.state.notified() => {}
                _ = timer.tick() => {}
            }
            if self.state.is_running() {
                self.maintain().await;
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// Each worker contributes only its open regions, refreshed at maintenance boundaries.
    fn refresh_usage(&mut self) {
        let usage = self
            .regions
            .list_regions()
            .iter()
            .map(|region| region.series_index_version().disk_usage())
            .sum();
        self.report_usage(usage);
    }

    fn report_usage(&mut self, usage: u64) {
        if usage >= self.reported_usage {
            let delta = usage - self.reported_usage;
            self.disk_usage.fetch_add(delta, Ordering::Relaxed);
            SERIES_INDEX_DISK_BYTES.add(delta as i64);
        } else {
            let delta = self.reported_usage - usage;
            self.disk_usage.fetch_sub(delta, Ordering::Relaxed);
            SERIES_INDEX_DISK_BYTES.sub(delta as i64);
        }
        self.reported_usage = usage;
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        self.refresh_usage();
        for region in self.regions.list_regions() {
            if !self.state.is_running() {
                break;
            }
            // Best effort: the region can still change state during reconciliation.
            // Local indexes can be built on followers as well as writable leaders.
            if !matches!(
                region.state(),
                RegionRoleState::Follower | RegionRoleState::Leader(RegionLeaderState::Writable)
            ) {
                continue;
            }
            // Full capacity defers builds, but cleanup must still reclaim published usage.
            let allow_builds = self.disk_usage.load(Ordering::Relaxed) < self.max_size;
            if !allow_builds {
                SERIES_INDEX_CAPACITY_DEFERRED.inc();
            }
            if let Err(error) = reconcile_series_indexes(
                self.worker_id,
                self.store.clone(),
                region.clone(),
                self.bucket_width,
                self.time_provider.current_time_millis(),
                self.purger.clone(),
                self.enable_range_index,
                allow_builds,
            )
            .await
            {
                SERIES_INDEX_RECONCILE_TOTAL
                    .with_label_values(&["failure"])
                    .inc();
                warn!(error; "Failed to reconcile series indexes, worker: {}, region: {}", self.worker_id, region.region_id);
            }
            self.refresh_usage();
        }
    }
}

impl Drop for SeriesIndexTask {
    fn drop(&mut self) {
        self.report_usage(0);
    }
}

#[cfg(test)]
mod tests {
    use object_store::services::Memory;
    use store_api::region_engine::RegionEngine;

    use super::*;
    use crate::region::RegionMap;
    use crate::series_index::catalog::{range_catalog_path, series_catalog_path};
    use crate::series_index::purger::series_index_channel;
    use crate::series_index::tests::prepare_region;
    use crate::test_util::TestEnv;

    #[rstest::rstest]
    #[case::follower(RegionRoleState::Follower, true)]
    #[case::writable(RegionRoleState::Leader(RegionLeaderState::Writable), true)]
    #[case::staging(RegionRoleState::Leader(RegionLeaderState::Staging), false)]
    #[case::entering_staging(RegionRoleState::Leader(RegionLeaderState::EnteringStaging), false)]
    #[case::altering(RegionRoleState::Leader(RegionLeaderState::Altering), false)]
    #[case::dropping(RegionRoleState::Leader(RegionLeaderState::Dropping), false)]
    #[case::truncating(RegionRoleState::Leader(RegionLeaderState::Truncating), false)]
    #[case::editing(RegionRoleState::Leader(RegionLeaderState::Editing), false)]
    #[case::downgrading(RegionRoleState::Leader(RegionLeaderState::Downgrading), false)]
    #[tokio::test]
    async fn test_maintenance_region_states(#[case] role: RegionRoleState, #[case] builds: bool) {
        let mut env = TestEnv::with_prefix("series-maintenance-state").await;
        let (engine, region) = prepare_region(&mut env).await;
        // Install the desired state without triggering the corresponding DDL.
        region.switch_state_to_staging(RegionLeaderState::Writable);
        region
            .manifest_ctx
            .exit_staging(region.region_id, role)
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, _receiver) = series_index_channel(store.clone());
        let regions = Arc::new(RegionMap::default());
        regions.insert_region(region.clone());
        let mut task = SeriesIndexTask {
            store: store.clone(),
            regions,
            bucket_width: Duration::from_secs(100),
            purger,
            disk_usage: Arc::default(),
            max_size: u64::MAX,
            reported_usage: 0,
            worker_id: 0,
            state: Arc::new(SeriesIndexTaskState::new()),
            interval: Duration::from_secs(3600),
            time_provider: Arc::new(crate::time_provider::StdTimeProvider),
            enable_range_index: true,
        };
        task.maintain().await;
        assert_eq!(
            builds,
            !region.series_index_version().series_indexes.is_empty()
        );
        for path in [
            range_catalog_path(region.region_id),
            series_catalog_path(region.region_id),
        ] {
            assert_eq!(builds, store.exists(&path).await.unwrap());
        }
        engine.stop().await.unwrap();
    }

    #[rstest::rstest]
    #[case::at_capacity(0, true)]
    #[case::above_capacity(1, true)]
    #[case::at_capacity_without_range(0, false)]
    #[case::above_capacity_without_range(1, false)]
    #[tokio::test]
    async fn test_full_capacity_cleanup_and_recovery(
        #[case] excess: u64,
        #[case] enable_range_index: bool,
    ) {
        use std::sync::Mutex;

        use object_store::layers::mock::MockLayerBuilder;

        use crate::series_index::catalog::load_version_control;
        use crate::series_index::tests::prepare_region_with_timestamps;
        use crate::time_provider::mock::MockTimeProvider;

        let mut env = TestEnv::with_prefix("series-capacity-cleanup").await;
        let (engine, region) =
            prepare_region_with_timestamps(&mut env, &[1000, 2000, 3000, 4000, 5000]).await;
        assert_eq!(
            5,
            region
                .version()
                .ssts
                .levels()
                .iter()
                .flat_map(|level| level.files())
                .count()
        );
        let mut options = region.version().options.clone();
        options.ttl = Some(common_time::TimeToLive::Duration(Duration::from_secs(100)));
        region.version_control.alter_options(options);
        let writes = Arc::new(Mutex::new(Vec::new()));
        let captured = writes.clone();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(
            MockLayerBuilder::default()
                .writer_factory(Arc::new(move |path, _, inner| {
                    captured.lock().unwrap().push(path.to_string());
                    inner
                }))
                .build()
                .unwrap(),
        );
        let (purger, mut receiver) = series_index_channel(store.clone());
        let regions = Arc::new(RegionMap::default());
        regions.insert_region(region.clone());
        let clock = Arc::new(MockTimeProvider::new(0));
        let usage = Arc::new(AtomicU64::new(0));
        let mut task = SeriesIndexTask {
            store: store.clone(),
            regions,
            bucket_width: Duration::from_secs(100),
            purger,
            disk_usage: usage.clone(),
            max_size: u64::MAX,
            reported_usage: 0,
            worker_id: 0,
            state: Arc::new(SeriesIndexTaskState::new()),
            interval: Duration::from_secs(3600),
            time_provider: clock.clone(),
            enable_range_index,
        };
        task.maintain().await;
        let previous = region.series_index_version();
        assert_eq!(1, previous.series_indexes.len());
        let old_id = *previous.series_indexes.keys().next().unwrap();
        task.max_size = previous.disk_usage() - excess;
        writes.lock().unwrap().clear();
        task.maintain().await;
        assert!(Arc::ptr_eq(&previous, &region.series_index_version()));
        assert!(writes.lock().unwrap().is_empty());

        // Removing the newest SST makes range coverage obsolete and would normally
        // trigger a series replacement over the four remaining SSTs.
        let sources = region.version();
        let newest = sources
            .ssts
            .levels()
            .iter()
            .flat_map(|level| level.files())
            .max_by_key(|file| file.meta_ref().sequence)
            .unwrap()
            .meta_ref()
            .clone();
        region.version_control.apply_edit(
            Some(crate::manifest::action::RegionEdit {
                files_to_remove: vec![newest.clone()],
                files_to_add: Vec::new(),
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: None,
            }),
            &[],
            crate::test_util::new_noop_file_purger(),
        );
        task.maintain().await;
        let cleaned = region.series_index_version();
        assert!(!cleaned.range_indexes.contains_key(&newest.file_id));
        assert_eq!(
            usize::from(enable_range_index) * 4,
            cleaned.range_indexes.len()
        );
        assert_eq!(previous.index_buckets, cleaned.index_buckets);
        assert_eq!(1, cleaned.series_indexes.len());
        assert!(cleaned.series_indexes.contains_key(&old_id));
        assert_eq!(cleaned.disk_usage(), usage.load(Ordering::Relaxed));
        assert_eq!(
            previous.disk_usage()
                - previous
                    .range_indexes
                    .get(&newest.file_id)
                    .map_or(0, |e| e.file_size),
            cleaned.disk_usage()
        );
        assert!(
            writes
                .lock()
                .unwrap()
                .iter()
                .all(|path| path == &range_catalog_path(region.region_id))
        );
        let restored = load_version_control(&store, region.region_id, &task.purger).await;
        assert_eq!(cleaned.range_indexes, restored.current().range_indexes);
        assert_eq!(cleaned.index_buckets, restored.current().index_buckets);
        drop(restored);

        // Expiration must work even when the shared estimate remains full.
        task.max_size = cleaned.disk_usage() - excess;
        clock.set_now(201_000);
        writes.lock().unwrap().clear();
        task.maintain().await;
        let expired = region.series_index_version();
        assert!(expired.series_indexes.is_empty());
        assert!(expired.index_buckets.is_empty());
        assert_eq!(cleaned.range_indexes, expired.range_indexes);
        assert_eq!(expired.disk_usage(), usage.load(Ordering::Relaxed));
        assert!(usage.load(Ordering::Relaxed) < task.max_size);
        assert_eq!(
            *writes.lock().unwrap(),
            vec![series_catalog_path(region.region_id)]
        );
        let restored = load_version_control(&store, region.region_id, &task.purger).await;
        assert!(restored.current().series_indexes.is_empty());
        assert_eq!(expired.range_indexes, restored.current().range_indexes);
        assert!(receiver.try_recv().is_err());
        drop(previous);
        assert!(receiver.try_recv().is_err());
        drop(cleaned);
        assert_eq!(old_id, receiver.try_recv().unwrap().file_id.file_id());
        assert!(receiver.try_recv().is_err());

        // Make the remaining SSTs eligible again; reclaimed capacity admits a build
        // without closing the region or increasing the configured limit.
        clock.set_now(0);
        task.maintain().await;
        let rebuilt = region.series_index_version();
        assert_eq!(1, rebuilt.series_indexes.len());
        assert!(!rebuilt.series_indexes.contains_key(&old_id));
        assert_eq!(
            4,
            rebuilt
                .series_indexes
                .values()
                .next()
                .unwrap()
                .entry()
                .source_file_ids
                .len()
        );
        assert_eq!(rebuilt.disk_usage(), usage.load(Ordering::Relaxed));
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_shared_usage_defers_builds_and_allows_overshoot() {
        let mut env = TestEnv::with_prefix("series-approximate-usage").await;
        let (engine, region) = prepare_region(&mut env).await;
        let usage = Arc::new(AtomicU64::new(0));
        let store = ObjectStore::new(Memory::default()).unwrap();
        let make_task = || SeriesIndexTask {
            store: store.clone(),
            regions: Arc::new(RegionMap::default()),
            bucket_width: Duration::from_secs(100),
            purger: series_index_channel(store.clone()).0,
            disk_usage: usage.clone(),
            max_size: 1024,
            reported_usage: 0,
            worker_id: 0,
            state: Arc::new(SeriesIndexTaskState::new()),
            interval: Duration::from_secs(3600),
            time_provider: Arc::new(crate::time_provider::StdTimeProvider),
            enable_range_index: true,
        };
        let mut task = make_task();
        task.regions.insert_region(region.clone());
        task.maintain().await;
        let bytes = region.series_index_version().disk_usage();
        assert!(
            bytes > task.max_size,
            "a started reconciliation may exceed the limit"
        );
        assert_eq!(bytes, usage.load(Ordering::Relaxed));
        // Another worker sees the same estimate and defers builds without creating coverage.
        let mut other_env = TestEnv::with_prefix("series-approximate-other").await;
        let (other_engine, other_region) = prepare_region(&mut other_env).await;
        let mut other = make_task();
        other.regions.insert_region(other_region.clone());
        other.max_size = bytes; // Exact equality also skips.
        other.maintain().await;
        assert_eq!(0, other_region.series_index_version().disk_usage());
        assert_eq!(bytes, usage.load(Ordering::Relaxed));

        // Closing a region reduces the estimate on the next pass, even while full.
        task.regions.remove_region(region.region_id);
        task.maintain().await;
        assert_eq!(0, usage.load(Ordering::Relaxed));
        other.maintain().await;
        assert!(other_region.series_index_version().disk_usage() > 0);
        assert_eq!(
            other_region.series_index_version().disk_usage(),
            usage.load(Ordering::Relaxed)
        );
        drop(other);
        assert_eq!(0, usage.load(Ordering::Relaxed));
        engine.stop().await.unwrap();
        other_engine.stop().await.unwrap();
    }
}
