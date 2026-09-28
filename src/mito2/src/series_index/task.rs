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
use snafu::ensure;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{InvalidRequestSnafu, RegionBusySnafu, Result, WorkerStoppedSnafu};
use crate::metrics::{
    SERIES_INDEX_CAPACITY_DEFERRED, SERIES_INDEX_DISK_BYTES, SERIES_INDEX_RECONCILE_TOTAL,
};
use crate::read::series_candidate::is_sparse_metric_metadata;
use crate::region::{MitoRegionRef, RegionLeaderState, RegionMapRef, RegionRoleState};
use crate::series_index::maintenance::{ReconcileStats, reconcile_series_indexes};
use crate::series_index::purger::{IndexFilePurger, PurgeRequest, run_index_purge_task};
use crate::time_provider::TimeProviderRef;

/// Commands serialized with periodic series-index maintenance.
#[derive(Debug)]
pub(crate) enum SeriesIndexCommand {
    Wake,
    Reconcile {
        region: MitoRegionRef,
        sender: oneshot::Sender<Result<ReconcileStats>>,
    },
    Stop,
}

/// Shared lifecycle state and command sender for a worker's series-index task.
#[derive(Debug)]
pub(crate) struct SeriesIndexTaskState {
    worker_id: u32,
    running: AtomicBool,
    sender: Sender<SeriesIndexCommand>,
}

impl SeriesIndexTaskState {
    pub(crate) fn new(worker_id: u32, channel_size: usize) -> (Self, Receiver<SeriesIndexCommand>) {
        let (sender, receiver) = mpsc::channel(channel_size);
        (
            Self {
                worker_id,
                running: AtomicBool::new(true),
                sender,
            },
            receiver,
        )
    }

    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn wake(&self) {
        // Queued work already wakes the task. Periodic maintenance still runs even
        // if the queued command only reconciles one region.
        if self.is_running() && self.sender.capacity() == self.sender.max_capacity() {
            let _ = self.sender.try_send(SeriesIndexCommand::Wake);
        }
    }

    /// Returns a receiver for build completion, or RegionBusy if the queue is full.
    pub(crate) fn try_reconcile(
        &self,
        region: MitoRegionRef,
    ) -> Result<oneshot::Receiver<Result<ReconcileStats>>> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.worker_id });
        let region_id = region.region_id;
        let (sender, receiver) = oneshot::channel();
        self.sender
            .try_send(SeriesIndexCommand::Reconcile { region, sender })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RegionBusySnafu { region_id }.build(),
                mpsc::error::TrySendError::Closed(_) => {
                    WorkerStoppedSnafu { id: self.worker_id }.build()
                }
            })?;
        Ok(receiver)
    }

    pub(crate) fn stop(&self) {
        if self.running.swap(false, Ordering::AcqRel) {
            // A full queue already wakes the task, which checks the running flag.
            let _ = self.sender.try_send(SeriesIndexCommand::Stop);
        }
    }
}

/// Starts both tasks on the compaction runtime, detaching purge and returning the maintenance handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    receiver: Receiver<SeriesIndexCommand>,
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
            receiver,
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
    receiver: Receiver<SeriesIndexCommand>,
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
        let mut deadline = Instant::now() + interval;
        while self.state.is_running() {
            // Check before receiving so a busy channel cannot starve periodic maintenance.
            if Instant::now() >= deadline {
                self.maintain().await;
                deadline = Instant::now() + interval;
                continue;
            }
            match tokio::time::timeout_at(deadline, self.receiver.recv()).await {
                Err(_) => continue,
                Ok(None | Some(SeriesIndexCommand::Stop)) => break,
                Ok(Some(SeriesIndexCommand::Wake)) => {
                    if self.state.is_running() {
                        self.maintain().await;
                        deadline = Instant::now() + interval;
                    }
                }
                Ok(Some(SeriesIndexCommand::Reconcile { region, sender })) => {
                    let result = self.reconcile_manual(region).await;
                    let _ = sender.send(result);
                }
            }
        }
        self.state.stop();
        self.receiver.close();
        while let Some(command) = self.receiver.recv().await {
            if let SeriesIndexCommand::Reconcile { sender, .. } = command {
                let _ = sender.send(WorkerStoppedSnafu { id: worker_id }.fail());
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// A manual request must still refer to the same writable region instance.
    fn validate_manual_region(&self, region: &MitoRegionRef) -> Result<()> {
        ensure!(
            self.state.is_running(),
            WorkerStoppedSnafu { id: self.worker_id }
        );
        let current = self.regions.writable_non_staging_region(region.region_id)?;
        ensure!(
            Arc::ptr_eq(&current, region),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "region was replaced during series-index reconciliation",
            }
        );
        ensure!(
            is_sparse_metric_metadata(&region.version().metadata),
            InvalidRequestSnafu {
                region_id: region.region_id,
                reason: "series indexes require sparse metric metadata",
            }
        );
        Ok(())
    }

    async fn reconcile_manual(&mut self, region: MitoRegionRef) -> Result<ReconcileStats> {
        self.validate_manual_region(&region)?;
        self.refresh_usage();
        let result = self.reconcile_region(region.clone()).await;
        self.refresh_usage();
        let result = result?;
        self.validate_manual_region(&region)?;
        Ok(result)
    }

    async fn reconcile_region(&self, region: MitoRegionRef) -> Result<ReconcileStats> {
        // Full capacity defers builds, but cleanup must still reclaim published usage.
        let allow_builds = self.disk_usage.load(Ordering::Relaxed) < self.max_size;
        if !allow_builds {
            SERIES_INDEX_CAPACITY_DEFERRED.inc();
        }
        let result = reconcile_series_indexes(
            self.worker_id,
            self.store.clone(),
            region,
            self.bucket_width,
            self.time_provider.current_time_millis(),
            self.purger.clone(),
            self.enable_range_index,
            allow_builds,
        )
        .await;
        if result.is_err() {
            SERIES_INDEX_RECONCILE_TOTAL
                .with_label_values(&["failure"])
                .inc();
        }
        result
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
            if let Err(error) = self.reconcile_region(region.clone()).await {
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
    use object_store::layers::mock::{self, MockLayerBuilder, oio};
    use object_store::services::Memory;
    use store_api::region_engine::{RegionEngine, RegionRole};
    use tokio::sync::Notify;

    use super::*;
    use crate::region::RegionMap;
    use crate::series_index::catalog::{range_catalog_path, series_catalog_path};
    use crate::series_index::purger::series_index_channel;
    use crate::series_index::tests::prepare_region;
    use crate::test_util::TestEnv;

    struct BlockingCatalogWriter {
        inner: oio::Writer,
        entered: Arc<Notify>,
        release: Arc<Notify>,
    }

    impl mock::Write for BlockingCatalogWriter {
        async fn write(&mut self, buffer: mock::Buffer) -> mock::Result<()> {
            self.inner.write(buffer).await
        }

        async fn close(&mut self) -> mock::Result<mock::Metadata> {
            self.entered.notify_one();
            self.release.notified().await;
            self.inner.close().await
        }

        async fn abort(&mut self) -> mock::Result<()> {
            self.inner.abort().await
        }
    }

    #[derive(Clone, Copy, Debug)]
    enum LifecycleChange {
        Remove,
        Demote,
        Stop,
    }

    fn new_task(region: MitoRegionRef) -> (SeriesIndexTask, UnboundedReceiver<PurgeRequest>) {
        let store = ObjectStore::new(Memory::default()).unwrap();
        let (purger, purge_receiver) = series_index_channel(store.clone());
        let regions = Arc::new(RegionMap::default());
        regions.insert_region(region);
        let (state, receiver) = SeriesIndexTaskState::new(0, 1);
        (
            SeriesIndexTask {
                store,
                regions,
                bucket_width: Duration::from_secs(100),
                purger,
                disk_usage: Arc::default(),
                max_size: u64::MAX,
                reported_usage: 0,
                worker_id: 0,
                state: Arc::new(state),
                receiver,
                interval: Duration::from_secs(3600),
                time_provider: Arc::new(crate::time_provider::StdTimeProvider),
                enable_range_index: true,
            },
            purge_receiver,
        )
    }

    #[test]
    fn test_coalesce_wakeups_and_stop() {
        let (state, mut receiver) = SeriesIndexTaskState::new(0, 2);
        state.wake();
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Wake
        ));
        assert!(receiver.try_recv().is_err());
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Wake
        ));
        state.stop();
        state.stop();
        state.wake();
        assert!(matches!(
            receiver.try_recv().unwrap(),
            SeriesIndexCommand::Stop
        ));
        assert!(receiver.try_recv().is_err());
    }

    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_manual_reconcile_publishes_before_reply(#[case] range_enabled: bool) {
        let mut env = TestEnv::with_prefix("series-manual").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (mut task, _purge_receiver) = new_task(region.clone());
        task.enable_range_index = range_enabled;
        let state = task.state.clone();
        let store = task.store.clone();
        let first = state.try_reconcile(region.clone()).unwrap();
        state.wake();
        assert_eq!(1, task.receiver.len());
        assert!(matches!(
            state.try_reconcile(region.clone()),
            Err(crate::error::Error::RegionBusy { .. })
        ));
        let handle = tokio::spawn(task.run());
        let first = tokio::time::timeout(Duration::from_secs(10), first)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        // Admission recovers after completion; both builds reuse the same publication.
        let second = state.try_reconcile(region.clone()).unwrap();
        let second = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(1, first.built_series);
        assert_eq!(if range_enabled { 4 } else { 0 }, first.built_range);
        assert_eq!(0, second.built_series);
        assert_eq!(0, second.built_range);
        assert_eq!(1, region.series_index_version().series_indexes.len());
        assert!(
            store
                .exists(&series_catalog_path(region.region_id))
                .await
                .unwrap()
        );
        assert_eq!(
            range_enabled,
            store
                .exists(&range_catalog_path(region.region_id))
                .await
                .unwrap()
        );
        state.stop();
        handle.await.unwrap();
        assert!(state.try_reconcile(region).is_err());
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_reconcile_respects_capacity_and_refreshes_usage() {
        let mut env = TestEnv::with_prefix("series-manual-capacity").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (mut task, _purge_receiver) = new_task(region.clone());
        task.max_size = 0;
        task.reconcile_manual(region.clone()).await.unwrap();
        assert_eq!(0, region.series_index_version().disk_usage());
        assert_eq!(0, task.disk_usage.load(Ordering::Relaxed));

        task.max_size = u64::MAX;
        task.reconcile_manual(region.clone()).await.unwrap();
        let bytes = region.series_index_version().disk_usage();
        assert!(bytes > 0);
        assert_eq!(bytes, task.disk_usage.load(Ordering::Relaxed));
        let usage = task.disk_usage.clone();
        drop(task);
        assert_eq!(0, usage.load(Ordering::Relaxed));
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_fails_queued_reconciliation() {
        let mut env = TestEnv::with_prefix("series-manual-stop").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (task, _purge_receiver) = new_task(region.clone());
        let state = task.state.clone();
        let result = state.try_reconcile(region.clone()).unwrap();
        assert!(matches!(
            state.try_reconcile(region.clone()),
            Err(crate::error::Error::RegionBusy { .. })
        ));
        state.stop();
        assert!(matches!(
            state.try_reconcile(region.clone()),
            Err(crate::error::Error::WorkerStopped { .. })
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            task.run().await;
            assert!(matches!(
                result.await.unwrap(),
                Err(crate::error::Error::WorkerStopped { .. })
            ));
        })
        .await
        .unwrap();
        assert!(region.series_index_version().series_indexes.is_empty());
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_manual_reconcile_rejects_closed_region() {
        let mut env = TestEnv::with_prefix("series-manual-closed").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (task, _purge_receiver) = new_task(region.clone());
        let result = task.state.try_reconcile(region.clone()).unwrap();
        task.regions.remove_region(region.region_id);
        let state = task.state.clone();
        let handle = tokio::spawn(task.run());
        assert!(matches!(
            result.await.unwrap(),
            Err(crate::error::Error::RegionNotFound { .. })
        ));
        assert!(region.series_index_version().series_indexes.is_empty());
        state.stop();
        handle.await.unwrap();
        engine.stop().await.unwrap();
    }

    #[rstest::rstest]
    #[case::remove(LifecycleChange::Remove)]
    #[case::demote(LifecycleChange::Demote)]
    #[case::stop(LifecycleChange::Stop)]
    #[tokio::test]
    async fn test_manual_reconcile_rejects_lifecycle_change_during_io(
        #[case] change: LifecycleChange,
    ) {
        let mut env = TestEnv::with_prefix("series-manual-lifecycle").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (mut task, _purge_receiver) = new_task(region.clone());
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let writer_entered = entered.clone();
        let writer_release = release.clone();
        let catalog_path = series_catalog_path(region.region_id);
        let layer = MockLayerBuilder::default()
            .writer_factory(Arc::new(move |path, _, inner| {
                if path == catalog_path {
                    Box::new(BlockingCatalogWriter {
                        inner,
                        entered: writer_entered.clone(),
                        release: writer_release.clone(),
                    })
                } else {
                    inner
                }
            }))
            .build()
            .unwrap();
        task.store = task.store.clone().layer(layer);
        let state = task.state.clone();
        let regions = task.regions.clone();
        let mut result = state.try_reconcile(region.clone()).unwrap();
        let handle = tokio::spawn(task.run());
        tokio::time::timeout(Duration::from_secs(10), entered.notified())
            .await
            .unwrap();
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        // A canceled queued caller still occupies capacity while catalog I/O is blocked.
        drop(state.try_reconcile(region.clone()).unwrap());
        for _ in 0..3 {
            assert!(matches!(
                state.try_reconcile(region.clone()),
                Err(crate::error::Error::RegionBusy { .. })
            ));
        }

        // The initial validation passed; invalidate the request during catalog I/O.
        match change {
            LifecycleChange::Remove => {
                regions.remove_region(region.region_id).unwrap();
            }
            LifecycleChange::Demote => region.set_role(RegionRole::Follower),
            LifecycleChange::Stop => state.stop(),
        }
        release.notify_one();
        let result = tokio::time::timeout(Duration::from_secs(10), result)
            .await
            .unwrap()
            .unwrap();
        state.stop();
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .unwrap()
            .unwrap();
        engine.stop().await.unwrap();

        match change {
            LifecycleChange::Remove => assert!(matches!(
                result,
                Err(crate::error::Error::RegionNotFound { .. })
            )),
            LifecycleChange::Demote => assert!(matches!(
                result,
                Err(crate::error::Error::RegionState { .. })
            )),
            LifecycleChange::Stop => assert!(matches!(
                result,
                Err(crate::error::Error::WorkerStopped { .. })
            )),
        }
    }

    #[tokio::test]
    async fn test_manual_traffic_does_not_postpone_periodic_maintenance() {
        let mut env = TestEnv::with_prefix("series-manual-deadline").await;
        let (engine, region) = prepare_region(&mut env).await;
        region.switch_state_to_staging(RegionLeaderState::Writable);
        region
            .manifest_ctx
            .exit_staging(region.region_id, RegionRoleState::Follower)
            .unwrap();
        let (mut task, _purge_receiver) = new_task(region.clone());
        task.interval = Duration::from_millis(20);
        let state = task.state.clone();
        let handle = tokio::spawn(task.run());
        // Manual builds on followers fail. Only a periodic sweep can publish coverage.
        tokio::time::timeout(Duration::from_secs(10), async {
            while region.series_index_version().series_indexes.is_empty() {
                assert!(
                    state
                        .try_reconcile(region.clone())
                        .unwrap()
                        .await
                        .unwrap()
                        .is_err()
                );
            }
        })
        .await
        .unwrap();
        state.stop();
        handle.await.unwrap();
        engine.stop().await.unwrap();
    }

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
        let (state, receiver) = SeriesIndexTaskState::new(0, 2);
        let mut task = SeriesIndexTask {
            store: store.clone(),
            regions,
            bucket_width: Duration::from_secs(100),
            purger,
            disk_usage: Arc::default(),
            max_size: u64::MAX,
            reported_usage: 0,
            worker_id: 0,
            state: Arc::new(state),
            receiver,
            interval: Duration::from_secs(3600),
            time_provider: Arc::new(crate::time_provider::StdTimeProvider),
            enable_range_index: true,
        };
        assert_eq!(
            role == RegionRoleState::Leader(RegionLeaderState::Writable),
            task.validate_manual_region(&region).is_ok(),
        );
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
        let (state, command_receiver) = SeriesIndexTaskState::new(0, 1);
        let mut task = SeriesIndexTask {
            store: store.clone(),
            regions,
            bucket_width: Duration::from_secs(100),
            purger,
            disk_usage: usage.clone(),
            max_size: u64::MAX,
            reported_usage: 0,
            worker_id: 0,
            state: Arc::new(state),
            receiver: command_receiver,
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
        let make_task = || {
            let (state, command_receiver) = SeriesIndexTaskState::new(0, 1);
            SeriesIndexTask {
                store: store.clone(),
                regions: Arc::new(RegionMap::default()),
                bucket_width: Duration::from_secs(100),
                purger: series_index_channel(store.clone()).0,
                disk_usage: usage.clone(),
                max_size: 1024,
                reported_usage: 0,
                worker_id: 0,
                state: Arc::new(state),
                receiver: command_receiver,
                interval: Duration::from_secs(3600),
                time_provider: Arc::new(crate::time_provider::StdTimeProvider),
                enable_range_index: true,
            }
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
