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
use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::metrics::SERIES_INDEX_RECONCILE_TOTAL;
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

/// Starts both tasks, detaching purge and returning the maintenance handle.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    store: ObjectStore,
    regions: RegionMapRef,
    state: Arc<SeriesIndexTaskState>,
    bucket_width: Duration,
    purger: IndexFilePurger,
    purge_receiver: UnboundedReceiver<PurgeRequest>,
    interval: Duration,
    time_provider: TimeProviderRef,
) -> JoinHandle<()> {
    // Snapshots may retain senders after the worker stops; purge until all senders drop.
    common_runtime::spawn_global(run_index_purge_task(
        worker_id,
        store.clone(),
        purge_receiver,
    ));
    common_runtime::spawn_global(async move {
        SeriesIndexTask {
            worker_id,
            store,
            regions,
            bucket_width,
            purger,
            state,
            interval,
            time_provider,
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
    interval: Duration,
    time_provider: TimeProviderRef,
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
            if let Err(error) = reconcile_series_indexes(
                self.worker_id,
                self.store.clone(),
                region.clone(),
                self.bucket_width,
                self.time_provider.current_time_millis(),
                self.purger.clone(),
            )
            .await
            {
                SERIES_INDEX_RECONCILE_TOTAL
                    .with_label_values(&["failure"])
                    .inc();
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
            worker_id: 0,
            state: Arc::new(SeriesIndexTaskState::new()),
            interval: Duration::from_secs(3600),
            time_provider: Arc::new(crate::time_provider::StdTimeProvider),
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
}
