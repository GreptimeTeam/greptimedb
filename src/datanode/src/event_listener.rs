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

use common_telemetry::error;
use store_api::storage::RegionId;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub enum RegionServerEvent {
    Register(RegionId),
    Deregister(RegionId),
}

pub trait RegionServerEventListener: Sync + Send {
    /// Called *after* a new region was created/opened.
    fn register_region(&self, _region_id: RegionId) {}

    /// Called *after* a region was closed.
    fn deregister_region(&self, _region_id: RegionId) {}
}

pub type RegionServerEventListenerRef = Box<dyn RegionServerEventListener>;

pub struct NoopRegionServerEventListener;

impl RegionServerEventListener for NoopRegionServerEventListener {}

#[derive(Debug, Clone)]
pub struct RegionServerEventSender(pub(crate) UnboundedSender<RegionServerEvent>);

impl RegionServerEventListener for RegionServerEventSender {
    fn register_region(&self, region_id: RegionId) {
        if let Err(e) = self.0.send(RegionServerEvent::Register(region_id)) {
            error!(
                "Failed to send registering region: {region_id} event, source: {}",
                e
            );
        }
    }

    fn deregister_region(&self, region_id: RegionId) {
        if let Err(e) = self.0.send(RegionServerEvent::Deregister(region_id)) {
            error!(
                "Failed to send deregistering region: {region_id} event, source: {}",
                e
            );
        }
    }
}

pub struct RegionServerEventReceiver(pub(crate) UnboundedReceiver<RegionServerEvent>);

pub fn new_region_server_event_channel() -> (RegionServerEventSender, RegionServerEventReceiver) {
    let (tx, rx) = mpsc::unbounded_channel();

    (RegionServerEventSender(tx), RegionServerEventReceiver(rx))
}
