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

use std::sync::{Arc, Mutex};

use store_api::storage::RegionId;
use tokio::sync::Notify;

/// Mito engine background event listener.
pub trait EventListener: Send + Sync {
    /// Notifies the listener that a region is flushed successfully.
    fn on_flush_success(&self, region_id: RegionId);
}

pub type EventListenerRef = Arc<dyn EventListener>;

/// Listener to watch flush events.
pub struct FlushListener {
    notify: Notify,
    last_flushed_region: Mutex<Option<RegionId>>,
}

impl FlushListener {
    /// Creates a new listener.
    pub fn new() -> FlushListener {
        FlushListener {
            notify: Notify::new(),
            last_flushed_region: Mutex::new(None),
        }
    }

    /// Wait until one flush job is done.
    pub async fn wait(&self) {
        self.notify.notified().await;
    }

    /// Returns the last flushed region.
    pub fn last_flushed_region(&self) -> Option<RegionId> {
        self.last_flushed_region.lock().unwrap().clone()
    }
}

impl EventListener for FlushListener {
    fn on_flush_success(&self, region_id: RegionId) {
        {
            let mut last_flushed_region = self.last_flushed_region.lock().unwrap();
            *last_flushed_region = Some(region_id);
        }

        self.notify.notify_one()
    }
}
