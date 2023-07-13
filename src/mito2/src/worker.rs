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

mod handle_create;
mod handle_open;
pub(crate) mod request;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use common_runtime::JoinHandle;
use common_telemetry::logging;
use futures::future::try_join_all;
use object_store::ObjectStore;
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, Mutex};

use crate::config::MitoConfig;
use crate::error::{JoinSnafu, Result, WorkerStoppedSnafu};
use crate::region::{RegionMap, RegionMapRef};
use crate::worker::request::{RequestBody, WorkerRequest};

/// Identifier for a worker.
pub(crate) type WorkerId = u32;

/// A fixed size group of [RegionWorkers](RegionWorker).
///
/// A worker group binds each region to a specific [RegionWorker] and sends
/// requests to region's dedicated worker.
///
/// ```mermaid
/// graph LR
///
/// WorkerRequest -- Route by region id --> Worker0 & Worker1
///
/// subgraph MitoEngine
///     subgraph WorkerGroup
///         Worker0["RegionWorker 0"]
///         Worker1["RegionWorker 1"]
///     end
/// end
///
/// Chan0[" Request channel 0"]
/// Chan1[" Request channel 1"]
/// WorkerThread1["RegionWorkerThread 1"]
///
/// subgraph WorkerThread0["RegionWorkerThread 0"]
///     subgraph RegionMap["RegionMap (regions bound to worker 0)"]
///         Region0["Region 0"]
///         Region2["Region 2"]
///     end
///     Buffer0["RequestBuffer"]
///
///     Buffer0 -- modify regions --> RegionMap
/// end
///
/// Worker0 --> Chan0
/// Worker1 --> Chan1
/// Chan0 --> Buffer0
/// Chan1 --> WorkerThread1
/// ```
#[derive(Debug)]
pub(crate) struct WorkerGroup {
    workers: Vec<RegionWorker>,
}

impl WorkerGroup {
    /// Start a worker group.
    ///
    /// The number of workers should be power of two.
    pub(crate) fn start<S: LogStore>(
        config: &MitoConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> WorkerGroup {
        assert!(config.num_workers.is_power_of_two());

        let workers = (0..config.num_workers)
            .map(|id| {
                RegionWorker::start(
                    WorkerConfig {
                        id: id as WorkerId,
                        channel_size: config.worker_channel_size,
                        request_batch_size: config.worker_request_batch_size,
                    },
                    log_store.clone(),
                    object_store.clone(),
                )
            })
            .collect();

        WorkerGroup { workers }
    }

    /// Stop the worker group.
    pub(crate) async fn stop(&self) -> Result<()> {
        logging::info!("Stop region worker group");

        try_join_all(self.workers.iter().map(|worker| worker.stop())).await?;

        Ok(())
    }

    /// Submit a request to a worker in the group.
    pub(crate) async fn submit_to_worker(&self, request: WorkerRequest) -> Result<()> {
        self.worker(request.body.region_id())
            .submit_request(request)
            .await
    }

    /// Get worker for specific `region_id`.
    fn worker(&self, region_id: RegionId) -> &RegionWorker {
        let mut hasher = DefaultHasher::new();
        region_id.hash(&mut hasher);
        let value = hasher.finish() as usize;
        let index = value_to_index(value, self.workers.len());

        &self.workers[index]
    }
}

fn value_to_index(value: usize, num_workers: usize) -> usize {
    value & (num_workers - 1)
}

/// Config for region worker.
#[derive(Debug, Clone)]
struct WorkerConfig {
    /// Id of the worker
    id: WorkerId,
    /// Capacity of the request channel.
    channel_size: usize,
    /// Batch size to process request.
    request_batch_size: usize,
}

/// Worker to write and alter regions bound to it.
#[derive(Debug)]
pub(crate) struct RegionWorker {
    /// Id of the worker.
    id: WorkerId,
    /// Regions bound to the worker.
    regions: RegionMapRef,
    /// Request sender.
    sender: Sender<WorkerRequest>,
    /// Handle to the worker thread.
    handle: Mutex<Option<JoinHandle<()>>>,
    /// Whether to run the worker thread.
    running: Arc<AtomicBool>,
}

impl RegionWorker {
    /// Start a region worker and its background thread.
    fn start<S: LogStore>(
        config: WorkerConfig,
        log_store: Arc<S>,
        object_store: ObjectStore,
    ) -> RegionWorker {
        let regions = Arc::new(RegionMap::default());
        let (sender, receiver) = mpsc::channel(config.channel_size);

        let running = Arc::new(AtomicBool::new(true));
        let mut worker_thread = RegionWorkerThread {
            id: config.id,
            regions: regions.clone(),
            receiver,
            log_store,
            object_store,
            running: running.clone(),
            request_batch_size: config.request_batch_size,
        };
        let handle = common_runtime::spawn_bg(async move {
            worker_thread.run().await;
        });

        RegionWorker {
            id: config.id,
            regions,
            sender,
            handle: Mutex::new(Some(handle)),
            running,
        }
    }

    /// Submit request to background worker thread.
    async fn submit_request(&self, request: WorkerRequest) -> Result<()> {
        ensure!(
            self.running.load(Ordering::Relaxed),
            WorkerStoppedSnafu { id: self.id }
        );
        ensure!(
            self.sender.send(request).await.is_ok(),
            WorkerStoppedSnafu { id: self.id }
        );

        Ok(())
    }

    /// Stop the worker.
    ///
    /// This method waits until the worker thread exists.
    async fn stop(&self) -> Result<()> {
        let handle = self.handle.lock().await.take();
        if let Some(handle) = handle {
            logging::info!("Stop region worker {}", self.id);

            self.running.store(false, Ordering::Relaxed);
            // TODO(yingwen): Send shutdown request.

            handle.await.context(JoinSnafu)?;
        }

        Ok(())
    }
}

impl Drop for RegionWorker {
    fn drop(&mut self) {
        if self.running.load(Ordering::Relaxed) {
            self.running.store(false, Ordering::Relaxed);
            // Once we drop the sender, the worker thread will receive a disconnected error.
        }
    }
}

type RequestBuffer = Vec<WorkerRequest>;

/// Background worker thread to handle requests.
struct RegionWorkerThread<S> {
    // Id of the worker.
    id: WorkerId,
    /// Regions bound to the worker.
    regions: RegionMapRef,
    /// Request receiver.
    receiver: Receiver<WorkerRequest>,
    // TODO(yingwen): Replaced by Wal.
    log_store: Arc<S>,
    /// Object store for manifest and SSTs.
    object_store: ObjectStore,
    /// Whether the worker thread is still running.
    running: Arc<AtomicBool>,
    /// Batch size to fetch requests from channel.
    request_batch_size: usize,
}

impl<S> RegionWorkerThread<S> {
    /// Starts the worker loop.
    async fn run(&mut self) {
        logging::info!("Start region worker thread {}", self.id);

        // Buffer to retrieve requests from receiver.
        let mut buffer = RequestBuffer::with_capacity(self.request_batch_size);

        while self.running.load(Ordering::Relaxed) {
            // Clear the buffer before handling next batch of requests.
            buffer.clear();

            match self.receiver.recv().await {
                Some(request) => buffer.push(request),
                None => break,
            }

            // Try to recv more requests from the channel.
            for _ in 1..buffer.capacity() {
                // We have received one request so we start from 1.
                match self.receiver.try_recv() {
                    Ok(req) => buffer.push(req),
                    // We still need to handle remaining requests.
                    Err(_) => break,
                }
            }

            self.handle_requests(&mut buffer).await;
        }

        logging::info!("Exit region worker thread {}", self.id);
    }

    /// Dispatches and processes requests.
    ///
    /// `buffer` should be empty.
    async fn handle_requests(&mut self, buffer: &mut RequestBuffer) {
        let mut dml_requests = Vec::with_capacity(buffer.len());
        let mut ddl_requests = Vec::with_capacity(buffer.len());
        for req in buffer.drain(..) {
            if req.body.is_ddl() {
                ddl_requests.push(req);
            } else {
                dml_requests.push(req);
            }
        }

        // Handles all dml requests first. So we can alter regions without
        // considering existing dml requests.
        self.handle_dml_requests(dml_requests).await;

        self.handle_ddl_requests(ddl_requests).await;
    }

    /// Takes and handles all dml requests.
    async fn handle_dml_requests(&mut self, write_requests: Vec<WorkerRequest>) {
        if write_requests.is_empty() {
            return;
        }

        // Create a write context that holds meta and sequence.

        unimplemented!()
    }

    /// Takes and handles all ddl requests.
    async fn handle_ddl_requests(&mut self, ddl_requests: Vec<WorkerRequest>) {
        if ddl_requests.is_empty() {
            return;
        }

        for request in ddl_requests {
            let res = match request.body {
                RequestBody::Create(req) => self.handle_create_request(req).await,
                RequestBody::Open(req) => self.handle_open_request(req).await,
                RequestBody::Write(_) => unreachable!(),
            };

            if let Some(sender) = request.sender {
                // Ignore send result.
                let _ = sender.send(res);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestEnv;

    #[test]
    fn test_value_to_index() {
        let num_workers = 1;
        for i in 0..10 {
            assert_eq!(0, value_to_index(i, num_workers));
        }

        let num_workers = 4;
        for i in 0..10 {
            assert_eq!(i % 4, value_to_index(i, num_workers));
        }
    }

    #[tokio::test]
    async fn test_worker_group_start_stop() {
        let env = TestEnv::new("group-stop");
        let group = env
            .create_worker_group(&MitoConfig {
                num_workers: 4,
                ..Default::default()
            })
            .await;

        group.stop().await.unwrap();
    }
}
