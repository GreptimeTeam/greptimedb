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

use std::sync::Arc;
use std::time::Duration;

use common_meta::DatanodeId;
use dashmap::DashMap;

/// How many region leases after a region's last failure detector sample the grace lasts.
const RESTART_GRACE_LEASES: u32 = 3;

pub type DatanodeRestartGraceRef = Arc<DatanodeRestartGrace>;

/// Keeps when the leader accepted each datanode's latest startup handshake.
///
/// A restarting datanode waits a region lease after its startup handshake before it opens
/// regions, so its first full heartbeat usually arrives after the failure detectors of its
/// regions have crossed the threshold. Failure detection skips a region whose detector was
/// last sampled before the startup handshake, until the detector receives a newer sample or
/// [RESTART_GRACE_LEASES] region leases have passed since the last sample.
pub struct DatanodeRestartGrace {
    period_millis: i64,
    startups: DashMap<DatanodeId, i64>,
}

impl DatanodeRestartGrace {
    pub(crate) fn new(region_lease: Duration) -> Self {
        Self {
            period_millis: (region_lease * RESTART_GRACE_LEASES).as_millis() as i64,
            startups: DashMap::new(),
        }
    }

    /// Records that the leader accepted a startup handshake of `datanode_id` at `ts_millis`.
    pub(crate) fn record_startup(&self, datanode_id: DatanodeId, ts_millis: i64) {
        self.startups.insert(datanode_id, ts_millis);
    }

    /// Returns whether a region on `datanode_id`, whose failure detector was last sampled at
    /// `last_heartbeat_millis`, is still within the restart grace at `now_millis`.
    pub(crate) fn is_restarting(
        &self,
        datanode_id: DatanodeId,
        last_heartbeat_millis: i64,
        now_millis: i64,
    ) -> bool {
        self.startups.get(&datanode_id).is_some_and(|startup| {
            last_heartbeat_millis < *startup
                && now_millis < last_heartbeat_millis + self.period_millis
        })
    }
}
