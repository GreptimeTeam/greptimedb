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
mod handle_manifest;
mod handle_open;
mod handle_truncate;
mod handle_write;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common_base::Plugins;
use common_runtime::JoinHandle;
use common_telemetry::{error, info, warn};
use futures::future::try_join_all;
use object_store::manager::ObjectStoreManagerRef;
use prometheus::IntGauge;
use rand::{thread_rng, Rng};
use snafu::{ensure, ResultExt};
use store_api::logstore::LogStore;
use store_api::region_engine::SetReadonlyResponse;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{mpsc, oneshot, watch, Mutex};

use crate::cache::write_cache::{WriteCache, WriteCacheRef};
use crate::cache::{CacheManager, CacheManagerRef};
use crate::compaction::CompactionScheduler;
use crate::config::MitoConfig;
use crate::error::{JoinSnafu, Result, WorkerStoppedSnafu};
use crate::flush::{FlushScheduler, WriteBufferManagerImpl, WriteBufferManagerRef};
use crate::memtable::MemtableBuilderProvider;
use crate::metrics::{REGION_COUNT, WRITE_STALL_TOTAL};
use crate::region::{MitoRegionRef, OpeningRegions, OpeningRegionsRef, RegionMap, RegionMapRef};
use crate::request::{
    BackgroundNotify, DdlRequest, SenderDdlRequest, SenderWriteRequest, WorkerRequest,
};
use crate::schedule::scheduler::{LocalScheduler, SchedulerRef};
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::puffin_manager::PuffinManagerFactory;
use crate::time_provider::{StdTimeProvider, TimeProviderRef};
use crate::wal::Wal;

/// Identifier for a worker.
pub(crate) type WorkerId = u32;

pub(crate) const DROPPING_MARKER_FILE: &str = ".dropping";

/// Interval to check whether regions should flush.
pub(crate) const CHECK_REGION_INTERVAL: Duration = Duration::from_secs(60);
/// Max delay to check region periodical tasks.
pub(crate) const MAX_INITIAL_CHECK_DELAY_SECS: u64 = 60 * 3;

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
    /// Scheduler for file purgers.
    purge_scheduler: SchedulerRef,
    /// Cache.
    cache_manager: CacheManagerRef,
}

impl WorkerGroup {
    /// Starts a worker group.
    ///
    /// The number of workers should be power of two.
    pub(crate) async fn start<S: LogStore>(
        config: Arc<MitoConfig>,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        plugins: Plugins,
    ) -> Result<WorkerGroup> {
        let (flush_sender, flush_receiver) = watch::channel(());
        let write_buffer_manager = Arc::new(
            WriteBufferManagerImpl::new(config.global_write_buffer_size.as_bytes() as usize)
                .with_notifier(flush_sender.clone()),
        );
        let puffin_manager_factory = PuffinManagerFactory::new(
            &config.index.aux_path,
            config.index.staging_size.as_bytes(),
            Some(config.index.write_buffer_size.as_bytes() as _),
        )
        .await?;
        let intermediate_manager = IntermediateManager::init_fs(&config.index.aux_path)
            .await?
            .with_buffer_size(Some(config.index.write_buffer_size.as_bytes() as _));
        let scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        // We use another scheduler to avoid purge jobs blocking other jobs.
        // A purge job is cheaper than other background jobs so they share the same job limit.
        let purge_scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        let write_cache = write_cache_from_config(
            &config,
            object_store_manager.clone(),
            puffin_manager_factory.clone(),
            intermediate_manager.clone(),
        )
        .await?;
        let cache_manager = Arc::new(
            CacheManager::builder()
                .sst_meta_cache_size(config.sst_meta_cache_size.as_bytes())
                .vector_cache_size(config.vector_cache_size.as_bytes())
                .page_cache_size(config.page_cache_size.as_bytes())
                .selector_result_cache_size(config.selector_result_cache_size.as_bytes())
                .index_metadata_size(config.inverted_index.metadata_cache_size.as_bytes())
                .index_content_size(config.inverted_index.content_cache_size.as_bytes())
                .write_cache(write_cache)
                .build(),
        );
        let time_provider = Arc::new(StdTimeProvider);

        let workers = (0..config.num_workers)
            .map(|id| {
                WorkerStarter {
                    id: id as WorkerId,
                    config: config.clone(),
                    log_store: log_store.clone(),
                    object_store_manager: object_store_manager.clone(),
                    write_buffer_manager: write_buffer_manager.clone(),
                    scheduler: scheduler.clone(),
                    purge_scheduler: purge_scheduler.clone(),
                    listener: WorkerListener::default(),
                    cache_manager: cache_manager.clone(),
                    puffin_manager_factory: puffin_manager_factory.clone(),
                    intermediate_manager: intermediate_manager.clone(),
                    time_provider: time_provider.clone(),
                    flush_sender: flush_sender.clone(),
                    flush_receiver: flush_receiver.clone(),
                    plugins: plugins.clone(),
                }
                .start()
            })
            .collect();

        Ok(WorkerGroup {
            workers,
            scheduler,
            purge_scheduler,
            cache_manager,
        })
    }

    /// Stops the worker group.
    pub(crate) async fn stop(&self) -> Result<()> {
        info!("Stop region worker group");

        // Stops the scheduler gracefully.
        self.scheduler.stop(true).await?;
        // Stops the purge scheduler gracefully.
        self.purge_scheduler.stop(true).await?;

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

    /// Returns true if the specific region is opening.
    pub(crate) fn is_region_opening(&self, region_id: RegionId) -> bool {
        self.worker(region_id).is_region_opening(region_id)
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
    pub(crate) fn worker(&self, region_id: RegionId) -> &RegionWorker {
        let index = region_id_to_index(region_id, self.workers.len());

        &self.workers[index]
    }
}

// Tests methods.
#[cfg(any(test, feature = "test"))]
impl WorkerGroup {
    /// Starts a worker group with `write_buffer_manager` and `listener` for tests.
    ///
    /// The number of workers should be power of two.
    pub(crate) async fn start_for_test<S: LogStore>(
        config: Arc<MitoConfig>,
        log_store: Arc<S>,
        object_store_manager: ObjectStoreManagerRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        listener: Option<crate::engine::listener::EventListenerRef>,
        time_provider: TimeProviderRef,
    ) -> Result<WorkerGroup> {
        let (flush_sender, flush_receiver) = watch::channel(());
        let write_buffer_manager = write_buffer_manager.unwrap_or_else(|| {
            Arc::new(
                WriteBufferManagerImpl::new(config.global_write_buffer_size.as_bytes() as usize)
                    .with_notifier(flush_sender.clone()),
            )
        });
        let scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        let purge_scheduler = Arc::new(LocalScheduler::new(config.max_background_jobs));
        let puffin_manager_factory = PuffinManagerFactory::new(
            &config.index.aux_path,
            config.index.staging_size.as_bytes(),
            Some(config.index.write_buffer_size.as_bytes() as _),
        )
        .await?;
        let intermediate_manager = IntermediateManager::init_fs(&config.index.aux_path)
            .await?
            .with_buffer_size(Some(config.index.write_buffer_size.as_bytes() as _));
        let write_cache = write_cache_from_config(
            &config,
            object_store_manager.clone(),
            puffin_manager_factory.clone(),
            intermediate_manager.clone(),
        )
        .await?;
        let cache_manager = Arc::new(
            CacheManager::builder()
                .sst_meta_cache_size(config.sst_meta_cache_size.as_bytes())
                .vector_cache_size(config.vector_cache_size.as_bytes())
                .page_cache_size(config.page_cache_size.as_bytes())
                .selector_result_cache_size(config.selector_result_cache_size.as_bytes())
                .write_cache(write_cache)
                .build(),
        );
        let workers = (0..config.num_workers)
            .map(|id| {
                WorkerStarter {
                    id: id as WorkerId,
                    config: config.clone(),
                    log_store: log_store.clone(),
                    object_store_manager: object_store_manager.clone(),
                    write_buffer_manager: write_buffer_manager.clone(),
                    scheduler: scheduler.clone(),
                    purge_scheduler: purge_scheduler.clone(),
                    listener: WorkerListener::new(listener.clone()),
                    cache_manager: cache_manager.clone(),
                    puffin_manager_factory: puffin_manager_factory.clone(),
                    intermediate_manager: intermediate_manager.clone(),
                    time_provider: time_provider.clone(),
                    flush_sender: flush_sender.clone(),
                    flush_receiver: flush_receiver.clone(),
                    plugins: Plugins::new(),
                }
                .start()
            })
            .collect();

        Ok(WorkerGroup {
            workers,
            scheduler,
            purge_scheduler,
            cache_manager,
        })
    }

    /// Returns the purge scheduler.
    pub(crate) fn purge_scheduler(&self) -> &SchedulerRef {
        &self.purge_scheduler
    }
}

fn region_id_to_index(id: RegionId, num_workers: usize) -> usize {
    ((id.table_id() as usize % num_workers) + (id.region_number() as usize % num_workers))
        % num_workers
}

async fn write_cache_from_config(
    config: &MitoConfig,
    object_store_manager: ObjectStoreManagerRef,
    puffin_manager_factory: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
) -> Result<Option<WriteCacheRef>> {
    if !config.enable_experimental_write_cache {
        return Ok(None);
    }

    // TODO(yingwen): Remove this and document the config once the write cache is ready.
    warn!("Write cache is an experimental feature");

    let cache = WriteCache::new_fs(
        &config.experimental_write_cache_path,
        object_store_manager,
        config.experimental_write_cache_size,
        config.experimental_write_cache_ttl,
        puffin_manager_factory,
        intermediate_manager,
    )
    .await?;
    Ok(Some(Arc::new(cache)))
}

/// Computes a initial check delay for a worker.
pub(crate) fn worker_init_check_delay() -> Duration {
    let init_check_delay = thread_rng().gen_range(0..MAX_INITIAL_CHECK_DELAY_SECS);
    Duration::from_secs(init_check_delay)
}

/// Worker start config.
struct WorkerStarter<S> {
    id: WorkerId,
    config: Arc<MitoConfig>,
    log_store: Arc<S>,
    object_store_manager: ObjectStoreManagerRef,
    write_buffer_manager: WriteBufferManagerRef,
    scheduler: SchedulerRef,
    purge_scheduler: SchedulerRef,
    listener: WorkerListener,
    cache_manager: CacheManagerRef,
    puffin_manager_factory: PuffinManagerFactory,
    intermediate_manager: IntermediateManager,
    time_provider: TimeProviderRef,
    /// Watch channel sender to notify workers to handle stalled requests.
    flush_sender: watch::Sender<()>,
    /// Watch channel receiver to wait for background flush job.
    flush_receiver: watch::Receiver<()>,
    plugins: Plugins,
}

impl<S: LogStore> WorkerStarter<S> {
    /// Starts a region worker and its background thread.
    fn start(self) -> RegionWorker {
        let regions = Arc::new(RegionMap::default());
        let opening_regions = Arc::new(OpeningRegions::default());
        let (sender, receiver) = mpsc::channel(self.config.worker_channel_size);

        let running = Arc::new(AtomicBool::new(true));
        let now = self.time_provider.current_time_millis();
        let id_string = self.id.to_string();
        let mut worker_thread = RegionWorkerLoop {
            id: self.id,
            config: self.config.clone(),
            regions: regions.clone(),
            dropping_regions: Arc::new(RegionMap::default()),
            opening_regions: opening_regions.clone(),
            sender: sender.clone(),
            receiver,
            wal: Wal::new(self.log_store),
            object_store_manager: self.object_store_manager.clone(),
            running: running.clone(),
            memtable_builder_provider: MemtableBuilderProvider::new(
                Some(self.write_buffer_manager.clone()),
                self.config.clone(),
            ),
            purge_scheduler: self.purge_scheduler.clone(),
            write_buffer_manager: self.write_buffer_manager,
            flush_scheduler: FlushScheduler::new(self.scheduler.clone()),
            compaction_scheduler: CompactionScheduler::new(
                self.scheduler,
                sender.clone(),
                self.cache_manager.clone(),
                self.config,
                self.listener.clone(),
                self.plugins.clone(),
            ),
            stalled_requests: StalledRequests::default(),
            listener: self.listener,
            cache_manager: self.cache_manager,
            puffin_manager_factory: self.puffin_manager_factory,
            intermediate_manager: self.intermediate_manager,
            time_provider: self.time_provider,
            last_periodical_check_millis: now,
            flush_sender: self.flush_sender,
            flush_receiver: self.flush_receiver,
            stalled_count: WRITE_STALL_TOTAL.with_label_values(&[&id_string]),
            region_count: REGION_COUNT.with_label_values(&[&id_string]),
        };
        let handle = common_runtime::spawn_global(async move {
            worker_thread.run().await;
        });

        RegionWorker {
            id: self.id,
            regions,
            opening_regions,
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
    /// The opening regions.
    opening_regions: OpeningRegionsRef,
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

    /// Returns true if the region is opening.
    fn is_region_opening(&self, region_id: RegionId) -> bool {
        self.opening_regions.is_region_exists(region_id)
    }

    /// Returns region of specific `region_id`.
    fn get_region(&self, region_id: RegionId) -> Option<MitoRegionRef> {
        self.regions.get_region(region_id)
    }

    #[cfg(test)]
    /// Returns the [OpeningRegionsRef].
    pub(crate) fn opening_regions(&self) -> &OpeningRegionsRef {
        &self.opening_regions
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
    /// Regions that are opening.
    opening_regions: OpeningRegionsRef,
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
    /// Memtable builder provider for each region.
    memtable_builder_provider: MemtableBuilderProvider,
    /// Background purge job scheduler.
    purge_scheduler: SchedulerRef,
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
    /// Puffin manager factory for index.
    puffin_manager_factory: PuffinManagerFactory,
    /// Intermediate manager for inverted index.
    intermediate_manager: IntermediateManager,
    /// Provider to get current time.
    time_provider: TimeProviderRef,
    /// Last time to check regions periodically.
    last_periodical_check_millis: i64,
    /// Watch channel sender to notify workers to handle stalled requests.
    flush_sender: watch::Sender<()>,
    /// Watch channel receiver to wait for background flush job.
    flush_receiver: watch::Receiver<()>,
    /// Gauge of stalled request count.
    stalled_count: IntGauge,
    /// Gauge of regions in the worker.
    region_count: IntGauge,
}

impl<S: LogStore> RegionWorkerLoop<S> {
    /// Starts the worker loop.
    async fn run(&mut self) {
        let init_check_delay = worker_init_check_delay();
        info!(
            "Start region worker thread {}, init_check_delay: {:?}",
            self.id, init_check_delay
        );
        self.last_periodical_check_millis += init_check_delay.as_millis() as i64;

        // Buffer to retrieve requests from receiver.
        let mut buffer = RequestBuffer::with_capacity(self.config.worker_request_batch_size);

        while self.running.load(Ordering::Relaxed) {
            // Clear the buffer before handling next batch of requests.
            buffer.clear();

            let max_wait_time = self.time_provider.wait_duration(CHECK_REGION_INTERVAL);
            let sleep = tokio::time::sleep(max_wait_time);
            tokio::pin!(sleep);

            tokio::select! {
                request_opt = self.receiver.recv() => {
                    match request_opt {
                        Some(request) => buffer.push(request),
                        // The channel is disconnected.
                        None => break,
                    }
                }
                recv_res = self.flush_receiver.changed() => {
                    if recv_res.is_err() {
                        // The channel is disconnected.
                        break;
                    } else {
                        // A flush job is finished, handles stalled requests.
                        self.handle_stalled_requests().await;
                        continue;
                    }
                }
                _ = &mut sleep => {
                    // Timeout. Checks periodical tasks.
                    self.handle_periodical_tasks();
                    continue;
                }
            }

            if self.flush_receiver.has_changed().unwrap_or(false) {
                // Always checks whether we could process stalled requests to avoid a request
                // hangs too long.
                // If the channel is closed, do nothing.
                self.handle_stalled_requests().await;
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

            self.listener.on_recv_requests(buffer.len());

            self.handle_requests(&mut buffer).await;

            self.handle_periodical_tasks();
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
                WorkerRequest::EditRegion {
                    region_id,
                    edit,
                    tx,
                } => {
                    self.handle_region_edit(region_id, edit, tx).await;
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
                DdlRequest::Open((req, wal_entry_receiver)) => {
                    self.handle_open_request(ddl.region_id, req, wal_entry_receiver, ddl.sender)
                        .await;
                    continue;
                }
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
                DdlRequest::Compact(req) => {
                    self.handle_compaction_request(ddl.region_id, req, ddl.sender)
                        .await;
                    continue;
                }
                DdlRequest::Truncate(_) => {
                    self.handle_truncate_request(ddl.region_id, ddl.sender)
                        .await;
                    continue;
                }
                DdlRequest::Catchup(req) => self.handle_catchup_request(ddl.region_id, req).await,
            };

            ddl.sender.send(res);
        }
    }

    /// Handle periodical tasks such as region auto flush.
    fn handle_periodical_tasks(&mut self) {
        let interval = CHECK_REGION_INTERVAL.as_millis() as i64;
        if self
            .time_provider
            .elapsed_since(self.last_periodical_check_millis)
            < interval
        {
            return;
        }

        self.last_periodical_check_millis = self.time_provider.current_time_millis();

        if let Err(e) = self.flush_periodically() {
            error!(e; "Failed to flush regions periodically");
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
            BackgroundNotify::Truncate(req) => self.handle_truncate_result(req).await,
            BackgroundNotify::RegionChange(req) => self.handle_manifest_region_change_result(req),
            BackgroundNotify::RegionEdit(req) => self.handle_region_edit_result(req),
        }
    }

    /// Handles `set_readonly_gracefully`.
    async fn set_readonly_gracefully(
        &mut self,
        region_id: RegionId,
        sender: oneshot::Sender<SetReadonlyResponse>,
    ) {
        if let Some(region) = self.regions.get_region(region_id) {
            // We need to do this in background as we need the manifest lock.
            common_runtime::spawn_global(async move {
                region.set_readonly_gracefully().await;

                let last_entry_id = region.version_control.current().last_entry_id;
                let _ = sender.send(SetReadonlyResponse::success(Some(last_entry_id)));
            });
        } else {
            let _ = sender.send(SetReadonlyResponse::NotFound);
        }
    }
}

impl<S> RegionWorkerLoop<S> {
    /// Cleans up the worker.
    async fn clean(&self) {
        // Closes remaining regions.
        let regions = self.regions.list_regions();
        for region in regions {
            region.stop().await;
        }

        self.regions.clear();
    }

    /// Notifies the whole group that a flush job is finished so other
    /// workers can handle stalled requests.
    fn notify_group(&mut self) {
        // Notifies all receivers.
        let _ = self.flush_sender.send(());
        // Marks the receiver in current worker as seen so the loop won't be waked up immediately.
        self.flush_receiver.borrow_and_update();
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

    pub(crate) async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_merge_ssts_finished(region_id).await;
        }
        // Avoid compiler warning.
        let _ = region_id;
    }

    pub(crate) fn on_recv_requests(&self, request_num: usize) {
        #[cfg(any(test, feature = "test"))]
        if let Some(listener) = &self.listener {
            listener.on_recv_requests(request_num);
        }
        // Avoid compiler warning.
        let _ = request_num;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::TestEnv;

    #[test]
    fn test_region_id_to_index() {
        let num_workers = 4;

        let region_id = RegionId::new(1, 2);
        let index = region_id_to_index(region_id, num_workers);
        assert_eq!(index, 3);

        let region_id = RegionId::new(2, 3);
        let index = region_id_to_index(region_id, num_workers);
        assert_eq!(index, 1);
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
