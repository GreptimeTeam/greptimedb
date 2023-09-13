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

use async_trait::async_trait;
use common_telemetry::info;
use store_api::storage::RegionId;
use tokio::sync::Notify;

/// Mito engine background event listener.
#[async_trait]
pub trait EventListener: Send + Sync {
    /// Notifies the listener that a region is flushed successfully.
    fn on_flush_success(&self, region_id: RegionId);

    /// Notifies the listener that the engine is stalled.
    fn on_write_stall(&self);

    async fn on_flush_begin(&self, region_id: RegionId);
}

pub type EventListenerRef = Arc<dyn EventListener>;

/// Listener to watch flush events.
pub struct FlushListener {
    notify: Notify,
}

impl FlushListener {
    /// Creates a new listener.
    pub fn new() -> FlushListener {
        FlushListener {
            notify: Notify::new(),
        }
    }

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

    fn on_write_stall(&self) {}

    async fn on_flush_begin(&self, _region_id: RegionId) {}
}

/// Listener to watch stall events.
pub struct StallListener {
    notify: Notify,
}

impl StallListener {
    /// Creates a new listener.
    pub fn new() -> StallListener {
        StallListener {
            notify: Notify::new(),
        }
    }

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

    async fn on_flush_begin(&self, _region_id: RegionId) {}
}

pub struct FlushTruncateListener {
    notify_flush: Notify,
    notify_truncate: Notify,
}

impl FlushTruncateListener {
    pub fn new() -> FlushTruncateListener {
        FlushTruncateListener {
            notify_flush: Notify::new(),
            notify_truncate: Notify::new(),
        }
    }

    pub fn notify_flush(&self) {
        self.notify_flush.notify_one();
    }

    pub async fn wait_truncate(&self) {
        self.notify_truncate.notified().await;
    }
}

#[async_trait]
impl EventListener for FlushTruncateListener {
    fn on_flush_success(&self, _region_id: RegionId) {}

    fn on_write_stall(&self) {}

    async fn on_flush_begin(&self, region_id: RegionId) {
        info!(
            "Region {} begin do flush, notify region to truncate",
            region_id
        );
        self.notify_truncate.notify_one();
        self.notify_flush.notified().await;
    }
}
