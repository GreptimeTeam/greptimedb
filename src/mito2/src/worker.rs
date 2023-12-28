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

mod handle_alter;
mod handle_catchup;
mod handle_close;
mod handle_compaction;
mod handle_create;
mod handle_drop;
mod handle_flush;
mod handle_open;
mod handle_truncate;
mod handle_write;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use futures::future::try_join_all;
use object_store::manager::ObjectStoreManagerRef;
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_engine::SetReadonlyResponse;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, Mutex};

use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::CompactionScheduler;
use crate::config::MitoConfig;
use crate::error::{JoinSnafu, Result, WorkerStoppedSnafu};
use crate::flush::{FlushScheduler, WriteBufferManagerImpl, WriteBufferManagerRef};
use crate::memtable::time_series::TimeSeriesMemtableBuilder;
use crate::memtable::MemtableBuilderRef;
use crate::region::{MitoRegionRef, RegionMap, RegionMapRef};
use crate::request::{
    BackgroundNotify, DdlRequest, SenderDdlRequest, SenderWriteRequest, WorkerRequest,
};
use crate::schedule::scheduler::{LocalScheduler, SchedulerRef};
use crate::wal::Wal;

/// Identifier for a worker.
pub(crate) type WorkerId = u32;

pub(crate) const DROPPING_MARKER_FILE: &str = ".dropping";

#[cfg_attr(doc, aquamarine::aquamarine)]
/// A fixed size group of [RegionWorkers](RegionWorker).
///
/// A worker group binds each region to a specific [RegionWorker] and sends
/// requests to region's dedicated worker.
///
/// ```mermaid
/// graph LR
///
/// RegionRequest -- Route by region id --> Worker0 & Worker1
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
/// WorkerThread1["RegionWorkerLoop 1"]
///
/// subgraph WorkerThread0["RegionWorkerLoop 0"]
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
pub(crate) struct WorkerGroup {
    /// Workers of the group.
    workers: Vec<RegionWorker>,
    /// Global background job scheduelr.
    scheduler: SchedulerRef,
    /// Cache.
    cache_manager: CacheManagerRef,
}

impl WorkerGroup {
    /// Starts a worker group.
    ///
    /// The number of workers should be power of two.
    pub(crate) fn start<S: LogStore>(
        config: Arc<MitoConfig>,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
    ) -> WorkerGroup {
        let write_buffer_manager = Arc::new(WriteBufferManagerImpl::new(
            config.global_write_buffer_size.as_bytes() as usize,
        ));
        let scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        let cache_manager = Arc::new(CacheManager::new(
            config.sst_meta_cache_size.as_bytes(),
            config.vector_cache_size.as_bytes(),
            config.page_cache_size.as_bytes(),
        ));

        let workers = (0..config.num_workers)
            .map(|id| {
                WorkerStarter {
                    id: id as WorkerId,
                    config: config.clone(),
                    log_store: log_store.clone(),
                    object_store_manager: object_store_manager.clone(),
                    write_buffer_manager: write_buffer_manager.clone(),
                    scheduler: scheduler.clone(),
                    listener: WorkerListener::default(),
                    cache_manager: cache_manager.clone(),
                }
                .start()
            })
            .collect();

        WorkerGroup {
            workers,
            scheduler,
            cache_manager,
        }
    }

    /// Stops the worker group.
    pub(crate) async fn stop(&self) -> Result<()> {
        info!("Stop region worker group");

        // Stops the scheduler gracefully.
        self.scheduler.stop(true).await?;

        try_join_all(self.workers.iter().map(|worker| worker.stop())).await?;

        Ok(())
    }

    /// Submits a request to a worker in the group.
    pub(crate) async fn submit_to_worker(
        &self,
        region_id: RegionId,
        request: WorkerRequest,
    ) -> Result<()> {
        self.worker(region_id).submit_request(request).await
    }

    /// Returns true if the specific region exists.
    pub(crate) fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.worker(region_id).is_region_exists(region_id)
    }

    /// Returns region of specific `region_id`.
    ///
    /// This method should not be public.
    pub(crate) fn get_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        self.worker(region_id).get_region(region_id)
    }

    /// Returns cache of the group.
    pub(crate) fn cache_manager(&self) -> CacheManagerRef {
        self.cache_manager.clone()
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

// Tests methods.
#[cfg(any(test, feature = "test"))]
impl WorkerGroup {
    /// Starts a worker group with `write_buffer_manager` and `listener` for tests.
    ///
    /// The number of workers should be power of two.
    pub(crate) fn start_for_test<S: LogStore>(
        config: Arc<MitoConfig>,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        listener: Option<crate::engine::listener::EventListenerRef>,
    ) -> WorkerGroup {
        let write_buffer_manager = write_buffer_manager.unwrap_or_else(|| {
            Arc::new(WriteBufferManagerImpl::new(
                config.global_write_buffer_size.as_bytes() as usize,
            ))
        });
        let scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        let cache_manager = Arc::new(CacheManager::new(
            config.sst_meta_cache_size.as_bytes(),
            config.vector_cache_size.as_bytes(),
            config.page_cache_size.as_bytes(),
        ));

        let workers = (0..config.num_workers)
            .map(|id| {
                WorkerStarter {
                    id: id as WorkerId,
                    config: config.clone(),
                    log_store: log_store.clone(),
                    object_store_manager: object_store_manager.clone(),
                    write_buffer_manager: write_buffer_manager.clone(),
                    scheduler: scheduler.clone(),
                    listener: WorkerListener::new(listener.clone()),
                    cache_manager: cache_manager.clone(),
                }
                .start()
            })
            .collect();

        WorkerGroup {
            workers,
            scheduler,
            cache_manager,
        }
    }
}

fn value_to_index(value: usize, num_workers: usize) -> usize {
    value % num_workers
}

/// Worker start config.
struct WorkerStarter<S> {
    id: WorkerId,
    config: Arc<MitoConfig>,
    log_store: Arc<S>,
    object_store_manager: ObjectStoreManagerRef,
    write_buffer_manager: WriteBufferManagerRef,
    scheduler: SchedulerRef,
    listener: WorkerListener,
    cache_manager: CacheManagerRef,
}

impl<S: LogStore> WorkerStarter<S> {
    /// Starts a region worker and its background thread.
    fn start(self) -> RegionWorker {
        let regions = Arc::new(RegionMap::default());
        let (sender, receiver) = mpsc::channel(self.config.worker_channel_size);

        let running = Arc::new(AtomicBool::new(true));
        let mut worker_thread = RegionWorkerLoop {
            id: self.id,
            config: self.config,
            regions: regions.clone(),
            dropping_regions: Arc::new(RegionMap::default()),
            sender: sender.clone(),
            receiver,
            wal: Wal::new(self.log_store),
            object_store_manager: self.object_store_manager.clone(),
            running: running.clone(),
            memtable_builder: Arc::new(TimeSeriesMemtableBuilder::new(Some(
                self.write_buffer_manager.clone(),
            ))),
            scheduler: self.scheduler.clone(),
            write_buffer_manager: self.write_buffer_manager,
            flush_scheduler: FlushScheduler::new(self.scheduler.clone()),
            compaction_scheduler: CompactionScheduler::new(
                self.scheduler,
                sender.clone(),
                self.cache_manager.clone(),
            ),
            stalled_requests: StalledRequests::default(),
            listener: self.listener,
            cache_manager: self.cache_manager,
        };
        let handle = common_runtime::spawn_write(async move {
            worker_thread.run().await;
        });

        RegionWorker {
            id: self.id,
            regions,
            sender,
            handle: Mutex::new(Some(handle)),
            running,
        }
    }
}

/// Worker to write and alter regions bound to it.
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
    /// Submits request to background worker thread.
    async fn submit_request(&self, request: WorkerRequest) -> Result<()> {
        ensure!(self.is_running(), WorkerStoppedSnafu { id: self.id });
        if self.sender.send(request).await.is_err() {
            warn!(
                "Worker {} is already exited but the running flag is still true",
                self.id
            );
            // Manually set the running flag to false to avoid printing more warning logs.
            self.set_running(false);
            return WorkerStoppedSnafu { id: self.id }.fail();
        }

        Ok(())
    }

    /// Stop the worker.
    ///
    /// This method waits until the worker thread exists.
    async fn stop(&self) -> Result<()> {
        let handle = self.handle.lock().await.take();
        if let Some(handle) = handle {
            info!("Stop region worker {}", self.id);

            self.set_running(false);
            if self.sender.send(WorkerRequest::Stop).await.is_err() {
                warn!("Worker {} is already exited before stop", self.id);
            }

            handle.await.context(JoinSnafu)?;
        }

        Ok(())
    }

    /// Returns true if the worker is still running.
    fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Sets whether the worker is still running.
    fn set_running(&self, value: bool) {
        self.running.store(value, Ordering::Relaxed)
    }

    /// Returns true if the worker contains specific region.
    fn is_region_exists(&self, region_id: RegionId) -> bool {
        self.regions.is_region_exists(region_id)
    }

    /// Returns region of specific `region_id`.
    fn get_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        self.regions.get_region(region_id)
    }
}

impl Drop for RegionWorker {
    fn drop(&mut self) {
        if self.is_running() {
            self.set_running(false);
            // Once we drop the sender, the worker thread will receive a disconnected error.
        }
    }
}

type RequestBuffer = Vec<WorkerRequest>;

/// Buffer for stalled write requests.
///
/// Maintains stalled write requests and their estimated size.
#[derive(Default)]
pub(crate) struct StalledRequests {
    /// Stalled requests.
    pub(crate) requests: Vec<SenderWriteRequest>,
    /// Estimated size of all stalled requests.
    pub(crate) estimated_size: usize,
}

impl StalledRequests {
    /// Appends stalled requests.
    pub(crate) fn append(&mut self, requests: &mut Vec<SenderWriteRequest>) {
        let size: usize = requests
            .iter()
            .map(|req| req.request.estimated_size())
            .sum();
        self.requests.append(requests);
        self.estimated_size += size;
    }
}

/// Background worker loop to handle requests.
struct RegionWorkerLoop<S> {
    /// Id of the worker.
    id: WorkerId,
    /// Engine config.
    config: Arc<MitoConfig>,
    /// Regions bound to the worker.
    regions: RegionMapRef,
    /// Regions that are not yet fully dropped.
    dropping_regions: RegionMapRef,
    /// Request sender.
    sender: Sender<WorkerRequest>,
    /// Request receiver.
    receiver: Receiver<WorkerRequest>,
    /// WAL of the engine.
    wal: Wal<S>,
    /// Manages object stores for manifest and SSTs.
    object_store_manager: ObjectStoreManagerRef,
    /// Whether the worker thread is still running.
    running: Arc<AtomicBool>,
    /// Memtable builder for each region.
    memtable_builder: MemtableBuilderRef,
    /// Background job scheduler.
    scheduler: SchedulerRef,
    /// Engine write buffer manager.
    write_buffer_manager: WriteBufferManagerRef,
    /// Schedules background flush requests.
    flush_scheduler: FlushScheduler,
    /// Scheduler for compaction tasks.
    compaction_scheduler: CompactionScheduler,
    /// Stalled write requests.
    stalled_requests: StalledRequests,
    /// Event listener for tests.
    listener: WorkerListener,
    /// Cache.
    cache_manager: CacheManagerRef,
}

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Starts the worker loop.
    async fn run(&mut self) {
        info!("Start region worker thread {}", self.id);

        // Buffer to retrieve requests from receiver.
        let mut buffer = RequestBuffer::with_capacity(self.config.worker_request_batch_size);

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

        self.clean().await;

        info!("Exit region worker thread {}", self.id);
    }

    /// Dispatches and processes requests.
    ///
    /// `buffer` should be empty.
    async fn handle_requests(&mut self, buffer: &mut RequestBuffer) {
        let mut write_requests = Vec::with_capacity(buffer.len());
        let mut ddl_requests = Vec::with_capacity(buffer.len());
        for worker_req in buffer.drain(..) {
            match worker_req {
                WorkerRequest::Write(sender_req) => {
                    write_requests.push(sender_req);
                }
                WorkerRequest::Ddl(sender_req) => {
                    ddl_requests.push(sender_req);
                }
                WorkerRequest::Background { region_id, notify } => {
                    // For background notify, we handle it directly.
                    self.handle_background_notify(region_id, notify).await;
                }
                WorkerRequest::SetReadonlyGracefully { region_id, sender } => {
                    self.set_readonly_gracefully(region_id, sender).await;
                }
                // We receive a stop signal, but we still want to process remaining
                // requests. The worker thread will then check the running flag and
                // then exit.
                WorkerRequest::Stop => {
                    debug_assert!(!self.running.load(Ordering::Relaxed));
                }
            }
        }

        // Handles all write requests first. So we can alter regions without
        // considering existing write requests.
        self.handle_write_requests(write_requests, true).await;

        self.handle_ddl_requests(ddl_requests).await;
    }

    /// Takes and handles all ddl requests.
    async fn handle_ddl_requests(&mut self, ddl_requests: Vec<SenderDdlRequest>) {
        if ddl_requests.is_empty() {
            return;
        }

        for ddl in ddl_requests {
            let res = match ddl.request {
                DdlRequest::Create(req) => self.handle_create_request(ddl.region_id, req).await,
                DdlRequest::Drop(_) => self.handle_drop_request(ddl.region_id).await,
                DdlRequest::Open(req) => self.handle_open_request(ddl.region_id, req).await,
                DdlRequest::Close(_) => self.handle_close_request(ddl.region_id).await,
                DdlRequest::Alter(req) => {
                    self.handle_alter_request(ddl.region_id, req, ddl.sender)
                        .await;
                    continue;
                }
                DdlRequest::Flush(req) => {
                    self.handle_flush_request(ddl.region_id, req, ddl.sender)
                        .await;
                    continue;
                }
                DdlRequest::Compact(_) => {
                    self.handle_compaction_request(ddl.region_id, ddl.sender);
                    continue;
                }
                DdlRequest::Truncate(_) => self.handle_truncate_request(ddl.region_id).await,
                DdlRequest::Catchup(req) => self.handle_catchup_request(ddl.region_id, req).await,
            };

            ddl.sender.send(res);
        }
    }

    /// Handles region background request
    async fn handle_background_notify(&mut self, region_id: RegionId, notify: BackgroundNotify) {
        match notify {
            BackgroundNotify::FlushFinished(req) => {
                self.handle_flush_finished(region_id, req).await
            }
            BackgroundNotify::FlushFailed(req) => self.handle_flush_failed(region_id, req).await,
            BackgroundNotify::CompactionFinished(req) => {
                self.handle_compaction_finished(region_id, req).await
            }
            BackgroundNotify::CompactionFailed(req) => self.handle_compaction_failure(req).await,
        }
    }

    /// Handles `set_readonly_gracefully`.
    async fn set_readonly_gracefully(
        &mut self,
        region_id: RegionId,
        sender: oneshot::Sender<SetReadonlyResponse>,
    ) {
        if let Some(region) = self.regions.get_region(region_id) {
            region.set_writable(false);

            let last_entry_id = region.version_control.current().last_entry_id;
            let _ = sender.send(SetReadonlyResponse::success(Some(last_entry_id)));
        } else {
            let _ = sender.send(SetReadonlyResponse::NotFound);
        }
    }
}

impl<S> RegionWorkerLoop<S> {
    // Clean up the worker.
    async fn clean(&self) {
        // Closes remaining regions.
        let regions = self.regions.list_regions();
        for region in regions {
            if let Err(e) = region.stop().await {
                error!(e; "Failed to stop region {}", region.region_id);
            }
        }

        self.regions.clear();
    }
}

/// Wrapper that only calls event listener in tests.
#[derive(Default, Clone)]
pub(crate) struct WorkerListener {
    #[cfg(any(test, feature = "test"))]
    listener: Option<crate::engine::listener::EventListenerRef>,
}

impl WorkerListener {
    #[cfg(any(test, feature = "test"))]
    pub(crate) fn new(
        listener: Option<crate::engine::listener::EventListenerRef>,
    ) -> WorkerListener {
        WorkerListener { listener }
    }

    /// Flush is finished successfully.
    pub(crate) fn on_flush_success(&self, region_id: RegionId) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_flush_success(region_id);
        }
        // Avoid compiler warning.
        let _ = region_id;
    }

    /// Engine is stalled.
    pub(crate) fn on_write_stall(&self) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_write_stall();
        }
    }

    pub(crate) async fn on_flush_begin(&self, region_id: RegionId) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_flush_begin(region_id).await;
        }
        // Avoid compiler warning.
        let _ = region_id;
    }

    pub(crate) fn on_later_drop_begin(&self, region_id: RegionId) -> Option<Duration> {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            return listener.on_later_drop_begin(region_id);
        }
        // Avoid compiler warning.
        let _ = region_id;
        None
    }

    /// On later drop task is finished.
    pub(crate) fn on_later_drop_end(&self, region_id: RegionId, removed: bool) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_later_drop_end(region_id, removed);
        }
        // Avoid compiler warning.
        let _ = region_id;
        let _ = removed;
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
        let env = TestEnv::with_prefix("group-stop");
        let group = env
            .create_worker_group(MitoConfig {
                num_workers: 4,
                ..Default::default()
            })
            .await;

        group.stop().await.unwrap();
    }
}
