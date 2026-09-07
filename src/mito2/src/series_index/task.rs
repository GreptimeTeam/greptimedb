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
use tokio::sync::{Notify, mpsc};
use tokio::time::{Instant, MissedTickBehavior};

use crate::series_index::purger::{PurgeRequest, purge_file};

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
        // notify_one() retains a permit if the task has not started waiting yet.
        self.notify.notify_one();
    }

    pub(crate) async fn notified(&self) {
        self.notify.notified().await;
    }
}

/// Owns the background work and pending file deletions for one region worker.
pub(crate) struct SeriesIndexTask {
    worker_id: u32,
    store: ObjectStore,
    state: Arc<SeriesIndexTaskState>,
    purge_receiver: mpsc::UnboundedReceiver<PurgeRequest>,
    retry_purges: Vec<PurgeRequest>,
}

impl SeriesIndexTask {
    pub(crate) fn new(
        worker_id: u32,
        store: ObjectStore,
        state: Arc<SeriesIndexTaskState>,
        purge_receiver: mpsc::UnboundedReceiver<PurgeRequest>,
    ) -> Self {
        Self {
            worker_id,
            store,
            state,
            purge_receiver,
            retry_purges: Vec::new(),
        }
    }

    /// Processes requests and periodic maintenance until the worker stops.
    pub(crate) async fn run(mut self) {
        let worker_id = self.worker_id;
        info!("Start series-index background task, worker: {worker_id}");
        let interval = Duration::from_secs(5 * 60);
        let mut timer = tokio::time::interval_at(Instant::now() + interval, interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
        while self.state.is_running() {
            tokio::select! {
                _ = self.state.notified() => {}
                Some(request) = self.purge_receiver.recv() => self.purge(request).await,
                _ = timer.tick() => {
                    if self.state.is_running() {
                        self.maintain().await;
                    }
                }
            }
        }
        while let Ok(request) = self.purge_receiver.try_recv() {
            self.retry_purges.push(request);
        }
        for request in self.retry_purges {
            let _ = purge_file(&self.store, request).await;
        }
        info!("Stop series-index background task, worker: {worker_id}");
    }

    /// Runs periodic maintenance independently of incoming deletion requests.
    async fn maintain(&mut self) {
        // TODO: Reconcile indexes and perform other periodic maintenance here.
        for request in std::mem::take(&mut self.retry_purges) {
            self.purge(request).await;
        }
    }

    async fn purge(&mut self, request: PurgeRequest) {
        if !purge_file(&self.store, request).await {
            self.retry_purges.push(request);
        }
    }
}
