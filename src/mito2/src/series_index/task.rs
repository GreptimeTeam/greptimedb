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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use common_telemetry::{info, warn};
use object_store::ObjectStore;
use snafu::ensure;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::error::{InvalidRequestSnafu, Result, WorkerStoppedSnafu};
use crate::metrics::SERIES_INDEX_RECONCILE_TOTAL;
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

    /// Waits for queue capacity, then returns a receiver for build completion.
    /// Call from a spawned task so admission cannot block the region worker.
    pub(crate) async fn reconcile(
        &self,
        region: MitoRegionRef,
    ) -> Result<oneshot::Receiver<Result<ReconcileStats>>> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.worker_id });
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(SeriesIndexCommand::Reconcile { region, sender })
            .await
            .map_err(|_| WorkerStoppedSnafu { id: self.worker_id }.build())?;
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

    async fn reconcile_manual(&self, region: MitoRegionRef) -> Result<ReconcileStats> {
        self.validate_manual_region(&region)?;
        let result = self.reconcile_region(region.clone()).await?;
        self.validate_manual_region(&region)?;
        Ok(result)
    }

    async fn reconcile_region(&self, region: MitoRegionRef) -> Result<ReconcileStats> {
        let result = reconcile_series_indexes(
            self.worker_id,
            self.store.clone(),
            region,
            self.bucket_width,
            self.time_provider.current_time_millis(),
            self.purger.clone(),
            self.enable_range_index,
        )
        .await;
        if result.is_err() {
            SERIES_INDEX_RECONCILE_TOTAL
                .with_label_values(&["failure"])
                .inc();
        }
        result
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
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
        }
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
        let first = state.reconcile(region.clone()).await.unwrap();
        state.wake();
        assert_eq!(1, task.receiver.len());
        let second = state.reconcile(region.clone());
        tokio::pin!(second);
        // Admission waits for capacity; both builds must reuse the same publication.
        assert!(futures::poll!(second.as_mut()).is_pending());
        let handle = tokio::spawn(task.run());
        let second = tokio::time::timeout(Duration::from_secs(10), second)
            .await
            .unwrap()
            .unwrap();
        let first = first.await.unwrap().unwrap();
        let second = second.await.unwrap().unwrap();
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
        assert!(state.reconcile(region).await.is_err());
        engine.stop().await.unwrap();
    }

    #[tokio::test]
    async fn test_shutdown_fails_queued_reconciliation() {
        let mut env = TestEnv::with_prefix("series-manual-stop").await;
        let (engine, region) = prepare_region(&mut env).await;
        let (task, _purge_receiver) = new_task(region.clone());
        let state = task.state.clone();
        let result = state.reconcile(region.clone()).await.unwrap();
        let waiting = state.reconcile(region.clone());
        tokio::pin!(waiting);
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        state.stop();
        tokio::time::timeout(Duration::from_secs(10), async {
            task.run().await;
            assert!(matches!(
                result.await.unwrap(),
                Err(crate::error::Error::WorkerStopped { .. })
            ));
            assert!(matches!(
                waiting.await,
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
        let result = task.state.reconcile(region.clone()).await.unwrap();
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
                        .reconcile(region.clone())
                        .await
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
}
