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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use common_meta::DatanodeId;
use common_meta::distributed_time_constants::META_LEASE_SECS;
use tokio::time::Instant;

pub type LeaseGrantTrackerRef = Arc<LeaseGrantTracker>;

/// Tracks, on a monotonic clock, when this metasrv last granted region leases to each datanode.
///
/// It only holds the grants since this metasrv became the leader. For the grants it misses, it
/// assumes that a former leader stops granting within [`META_LEASE_SECS`] after that, as its
/// election lease has expired by then. This is not enforced: a former leader that stalls before
/// it notices the loss of its leadership can grant later.
#[derive(Debug)]
pub struct LeaseGrantTracker {
    inner: RwLock<LeaseGrants>,
}

#[derive(Debug)]
struct LeaseGrants {
    /// When this metasrv became the leader.
    leader_since: Instant,
    /// When this metasrv last granted region leases to each datanode.
    granted_at: HashMap<DatanodeId, Instant>,
}

impl Default for LeaseGrantTracker {
    fn default() -> Self {
        Self {
            inner: RwLock::new(LeaseGrants {
                leader_since: Instant::now(),
                granted_at: HashMap::new(),
            }),
        }
    }
}

impl LeaseGrantTracker {
    /// Forgets all grants and marks now as the instant this metasrv becomes the leader.
    pub fn reset(&self) {
        let mut inner = self.inner.write().unwrap();
        inner.leader_since = Instant::now();
        inner.granted_at.clear();
    }

    /// Records that region leases are granted to `datanode_id` now.
    pub fn record_grant(&self, datanode_id: DatanodeId) {
        let mut inner = self.inner.write().unwrap();
        // Takes the instant under the lock, so concurrent heartbeats of a datanode cannot
        // overwrite a later grant with an earlier one.
        inner.granted_at.insert(datanode_id, Instant::now());
    }

    /// Returns when the region leases granted to `datanode_id` have expired, given the lease
    /// duration `region_lease` and the assumption about former leaders above.
    pub fn lease_expiry(&self, datanode_id: DatanodeId, region_lease: Duration) -> Instant {
        let inner = self.inner.read().unwrap();
        let former_leader_grants = inner.leader_since + Duration::from_secs(META_LEASE_SECS);
        let last_grant = inner
            .granted_at
            .get(&datanode_id)
            .map_or(former_leader_grants, |granted_at| {
                (*granted_at).max(former_leader_grants)
            });
        last_grant + region_lease
    }
}
