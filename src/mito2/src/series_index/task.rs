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

use common_telemetry::info;
use object_store::ObjectStore;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use crate::series_index::purger::{PurgeRequest, run_index_purge_task};

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
pub(crate) fn spawn_series_index_tasks(
    worker_id: u32,
    store: ObjectStore,
    state: Arc<SeriesIndexTaskState>,
    purge_receiver: async_channel::Receiver<PurgeRequest>,
    interval: Duration,
) -> JoinHandle<()> {
    common_runtime::spawn_global(run_index_purge_task(
        worker_id,
        store,
        purge_receiver.clone(),
    ));
    common_runtime::spawn_global(async move {
        SeriesIndexTask {
            worker_id,
            state,
            interval,
        }
        .run()
        .await;
        // Snapshots may retain senders after the worker stops.
        purge_receiver.close();
    })
}

/// Periodic series-index maintenance for one region worker.
struct SeriesIndexTask {
    worker_id: u32,
    state: Arc<SeriesIndexTaskState>,
    interval: Duration,
}

impl SeriesIndexTask {
    /// Runs periodic maintenance until the worker stops.
    async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = self.interval;
        let mut timer = tokio::time::interval_at(Instant::now() + interval, interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while self.state.is_running() {
            tokio::select! {
                _ = self.state.notified() => {}
                _ = timer.tick() => {
                    if self.state.is_running() {
                        self.maintain().await;
                    }
                }
            }
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        // TODO: Reconcile indexes and perform other periodic maintenance here.
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use object_store::ObjectStore;
    use object_store::services::Memory;

    use crate::series_index::purger::series_index_channel;
    use crate::series_index::task::{SeriesIndexTaskState, spawn_series_index_tasks};

    #[tokio::test]
    async fn test_stop_closes_purge_channel() {
        let store = ObjectStore::new(Memory::default()).unwrap().finish();
        let (_purger, receiver) = series_index_channel(store.clone());
        let state = Arc::new(SeriesIndexTaskState::new());
        state.stop();
        spawn_series_index_tasks(0, store, state, receiver.clone(), Duration::from_secs(30))
            .await
            .unwrap();
        assert!(receiver.is_closed());
    }
}
