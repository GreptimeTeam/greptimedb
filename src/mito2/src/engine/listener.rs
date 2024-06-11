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

//! Engine event listener for tests.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use common_telemetry::info;
use store_api::storage::RegionId;
use tokio::sync::Notify;

/// Mito engine background event listener.
#[async_trait]
pub trait EventListener: Send + Sync {
    /// Notifies the listener that a region is flushed successfully.
    fn on_flush_success(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the engine is stalled.
    fn on_write_stall(&self) {}

    /// Notifies the listener that the region starts to do flush.
    async fn on_flush_begin(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the later drop task starts running.
    /// Returns the gc interval if we want to override the default one.
    fn on_later_drop_begin(&self, region_id: RegionId) -> Option<Duration> {
        let _ = region_id;
        None
    }

    /// Notifies the listener that the later drop task of the region is finished.
    fn on_later_drop_end(&self, region_id: RegionId, removed: bool) {
        let _ = region_id;
        let _ = removed;
    }

    /// Notifies the listener that ssts has been merged and the region
    /// is going to update its manifest.
    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        let _ = region_id;
    }

    /// Notifies the listener that the worker receives requests from the request channel.
    fn on_recv_requests(&self, request_num: usize) {
        let _ = request_num;
    }
}

pub type EventListenerRef = Arc<dyn EventListener>;

/// Listener to watch flush events.
#[derive(Default)]
pub struct FlushListener {
    notify: Notify,
}

impl FlushListener {
    /// Wait until one flush job is done.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl EventListener for FlushListener {
    fn on_flush_success(&self, region_id: RegionId) {
        info!("Region {} flush successfully", region_id);

        self.notify.notify_one()
    }
}

/// Listener to watch stall events.
#[derive(Default)]
pub struct StallListener {
    notify: Notify,
}

impl StallListener {
    /// Wait for a stall event.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl EventListener for StallListener {
    fn on_flush_success(&self, _region_id: RegionId) {}

    fn on_write_stall(&self) {
        info!("Engine is stalled");

        self.notify.notify_one();
    }
}

/// Listener to watch begin flush events.
///
/// Creates a background thread to execute flush region, and the main thread calls `wait_truncate()`
/// to block and wait for `on_flush_region()`.
/// When the background thread calls `on_flush_begin()`, the main thread is notified to truncate
/// region, and background thread thread blocks and waits for `notify_flush()` to continue flushing.
#[derive(Default)]
pub struct FlushTruncateListener {
    /// Notify flush operation.
    notify_flush: Notify,
    /// Notify truncate operation.
    notify_truncate: Notify,
}

impl FlushTruncateListener {
    /// Notify flush region to proceed.
    pub fn notify_flush(&self) {
        self.notify_flush.notify_one();
    }

    /// Wait for a truncate event.
    pub async fn wait_truncate(&self) {
        self.notify_truncate.notified().await;
    }
}

#[async_trait]
impl EventListener for FlushTruncateListener {
    /// Calling this function will block the thread!
    /// Notify the listener to perform a truncate region and block the flush region job.
    async fn on_flush_begin(&self, region_id: RegionId) {
        info!(
            "Region {} begin do flush, notify region to truncate",
            region_id
        );
        self.notify_truncate.notify_one();
        self.notify_flush.notified().await;
    }
}

/// Listener on dropping.
pub struct DropListener {
    gc_duration: Duration,
    notify: Notify,
}

impl DropListener {
    /// Creates a new listener with specific `gc_duration`.
    pub fn new(gc_duration: Duration) -> Self {
        DropListener {
            gc_duration,
            notify: Notify::new(),
        }
    }

    /// Waits until later drop task is done.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }
}

#[async_trait]
impl EventListener for DropListener {
    fn on_later_drop_begin(&self, _region_id: RegionId) -> Option<Duration> {
        Some(self.gc_duration)
    }

    fn on_later_drop_end(&self, _region_id: RegionId, removed: bool) {
        // Asserts result.
        assert!(removed);
        self.notify.notify_one();
    }
}

/// Listener on handling compaction requests.
#[derive(Default)]
pub struct CompactionListener {
    handle_finished_notify: Notify,
    blocker: Notify,
}

impl CompactionListener {
    /// Waits for handling compaction finished request.
    pub async fn wait_handle_finished(&self) {
        self.handle_finished_notify.notified().await;
    }

    /// Wakes up the listener.
    pub fn wake(&self) {
        self.blocker.notify_one();
    }
}

#[async_trait]
impl EventListener for CompactionListener {
    async fn on_merge_ssts_finished(&self, region_id: RegionId) {
        info!("Handle compaction finished request, region {region_id}");

        self.handle_finished_notify.notify_one();

        // Blocks current task.
        self.blocker.notified().await;
    }
}

/// Listener to block on flush and alter.
#[derive(Default)]
pub struct AlterFlushListener {
    flush_begin_notify: Notify,
    block_flush_notify: Notify,
    request_begin_notify: Notify,
}

impl AlterFlushListener {
    /// Waits on flush begin.
    pub async fn wait_flush_begin(&self) {
        self.flush_begin_notify.notified().await;
    }

    /// Waits on request begin.
    pub async fn wait_request_begin(&self) {
        self.request_begin_notify.notified().await;
    }

    /// Continue the flush job.
    pub fn wake_flush(&self) {
        self.block_flush_notify.notify_one();
    }
}

#[async_trait]
impl EventListener for AlterFlushListener {
    async fn on_flush_begin(&self, region_id: RegionId) {
        info!("Wait on notify to start flush for region {}", region_id);

        self.flush_begin_notify.notify_one();
        self.block_flush_notify.notified().await;

        info!("region {} begin flush", region_id);
    }

    fn on_recv_requests(&self, request_num: usize) {
        info!("receive {} request", request_num);

        self.request_begin_notify.notify_one();
    }
}
