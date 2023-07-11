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

//! Structs and utilities for writing regions.

mod request;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common_runtime::JoinHandle;
use common_telemetry::logging;
use object_store::ObjectStore;
use snafu::ResultExt;
use store_api::logstore::LogStore;
use tokio::sync::Mutex;

use crate::error::{JoinSnafu, Result};
use crate::region::{RegionMap, RegionMapRef};
use crate::worker::request::{Receiver, Sender, WorkerRequest};

/// A fixed size group of [RegionWorkers](RegionWorker).
///
/// The group binds each region to a specific [RegionWorker].
#[derive(Debug, Default)]
pub(crate) struct WorkerGroup {
    workers: Vec<RegionWorker>,
}

/// Identifier for a worker.
type WorkerId = u32;

/// Worker to write and alter regions bound to it.
#[derive(Debug)]
struct RegionWorker {
    // Id of the worker.
    id: WorkerId,
    /// Regions bound to the worker.
    regions: RegionMapRef,
    /// Request sender.
    sender: Sender,
    /// Handle to the worker thread.
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Whether to run the worker thread.
    running: Arc<AtomicBool>,
}

impl RegionWorker {
    /// Start a region worker and its background thread.
    fn start<S: LogStore>(
        id: WorkerId,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> RegionWorker {
        let regions = Arc::new(RegionMap::default());
        let (sender, receiver) = request::request_channel();

        let running = Arc::new(AtomicBool::new(true));
        let mut worker_thread = RegionWorkerThread {
            id,
            regions: regions.clone(),
            receiver,
            log_store,
            object_store,
            running: running.clone(),
        };
        let handle = common_runtime::spawn_bg(async move {
            worker_thread.run().await;
        });

        RegionWorker {
            id,
            regions,
            sender,
            handle: Mutex::new(Some(handle)),
            running,
        }
    }

    /// Stop the worker.
    ///
    /// This method waits until the worker thread exists.
    async fn stop(&self) -> Result<()> {
        let handle = self.handle.lock().await.take();
        if let Some(handle) = handle {
            logging::info!("Stop region worker {}", self.id);

            self.running.store(false, Ordering::Relaxed);
            self.sender.notify();

            handle.await.context(JoinSnafu)?;
        }

        Ok(())
    }
}

impl Drop for RegionWorker {
    fn drop(&mut self) {
        if self.running.load(Ordering::Relaxed) {
            self.running.store(false, Ordering::Relaxed);
            // Notify worker thread.
            self.sender.notify();
        }
    }
}

/// Background worker thread to handle requests.
struct RegionWorkerThread<S> {
    // Id of the worker.
    id: WorkerId,
    /// Regions bound to the worker.
    regions: RegionMapRef,
    /// Request receiver.
    receiver: Receiver,
    // TODO(yingwen): Replaced by Wal.
    log_store: Arc<S>,
    object_store: ObjectStore,
    // Whether the worker thread is still running.
    running: Arc<AtomicBool>,
}

impl<S> RegionWorkerThread<S> {
    /// Starts the worker loop.
    async fn run(&mut self) {
        // Buffer to retrieve requests from receiver.
        let mut buffer = Vec::new();

        while self.running.load(Ordering::Relaxed) {
            // Clear the buffer before handling next batch of requests.
            buffer.clear();

            if let Err(e) = self.handle_requests(&mut buffer).await {
                logging::error!(e; "Failed to handle requests, worker: {}", self.id);
            }
        }
    }

    /// Process requests.
    ///
    /// `buffer` should be empty.
    async fn handle_requests(&mut self, buffer: &mut Vec<WorkerRequest>) -> Result<()> {
        self.receiver.receive_all(buffer).await;

        // let region_requests = expr;

        todo!()
    }
}
