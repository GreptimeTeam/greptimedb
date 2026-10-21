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

//! Leases the metasrv grants, which bound when a region acknowledges writes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use store_api::region_engine::{RegionLeaseGrant, RegionRole};

/// Lease of this node, renewed by every heartbeat response that carries region leases.
///
/// A region created on this node writes under it until the region gets its first grant.
#[derive(Debug, Default)]
pub(crate) struct NodeLease {
    /// Whether writes need a valid lease. Only a datanode the metasrv controls sets it.
    enabled: AtomicBool,
    /// The latest deadline and the lease it was computed with.
    renewal: Mutex<Option<(Instant, Duration)>>,
}

pub(crate) type NodeLeaseRef = Arc<NodeLease>;

impl NodeLease {
    /// Makes writes need a valid lease.
    pub(crate) fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    /// Extends the deadline to `heartbeat_sent_at + lease` if that is later.
    pub(crate) fn renew(&self, heartbeat_sent_at: Instant, lease: Duration) {
        let deadline = heartbeat_sent_at + lease;
        let mut renewal = self.renewal.lock().unwrap();
        if renewal.is_none_or(|(current, _)| current < deadline) {
            *renewal = Some((deadline, lease));
        }
    }

    /// Returns whether writes need a valid lease.
    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Returns the deadline of a region created at `created_at` that has no grant yet: the
    /// node deadline, within one lease of the creation.
    fn created_region_deadline(&self, created_at: Instant) -> Option<Instant> {
        self.renewal
            .lock()
            .unwrap()
            .map(|(deadline, lease)| deadline.min(created_at + lease))
    }
}

/// Leases the metasrv grants to a region. A catchup that reopens the region keeps it.
#[derive(Debug, Default)]
pub(crate) struct RegionLeaseRecord {
    state: Mutex<RegionLeaseState>,
}

pub(crate) type RegionLeaseRecordRef = Arc<RegionLeaseRecord>;

#[derive(Debug, Default)]
struct RegionLeaseState {
    /// Role of the latest grant.
    role: Option<RegionRole>,
    /// Deadline of the latest Leader or StagingLeader grant.
    leader_deadline: Option<Instant>,
    /// When the region was created on this node.
    created_at: Option<Instant>,
    /// When the metasrv last demoted the region.
    demoted_at: Option<Instant>,
}

impl RegionLeaseRecord {
    /// Returns the record of a region created on this node at `now`.
    pub(crate) fn created(now: Instant) -> Self {
        Self {
            state: Mutex::new(RegionLeaseState {
                created_at: Some(now),
                ..Default::default()
            }),
        }
    }

    /// Records `grant` and applies its role with `set_role`, both under the record's lock so
    /// that a demotion cannot fall in between. A leader grant for a heartbeat sent before the
    /// last demotion is stale: it is ignored and the method returns false.
    pub(crate) fn grant(&self, grant: RegionLeaseGrant, set_role: impl FnOnce(RegionRole)) -> bool {
        let mut state = self.state.lock().unwrap();
        if grant.role.writable()
            && state
                .demoted_at
                .is_some_and(|demoted_at| grant.heartbeat_sent_at < demoted_at)
        {
            return false;
        }
        state.role = Some(grant.role);
        if grant.role.writable() {
            state.leader_deadline = Some(grant.deadline);
        }
        set_role(grant.role);
        true
    }

    /// Records that the metasrv demotes the region at `now`.
    pub(crate) fn demote(&self, now: Instant) {
        self.state.lock().unwrap().demoted_at = Some(now);
    }

    /// Returns whether the region may take a write at `now`: its latest grant, if any, is Leader
    /// or StagingLeader, and the Leader deadline has not passed.
    pub(crate) fn allows_write(&self, node: &NodeLease, now: Instant) -> bool {
        self.is_valid(node, now, true)
    }

    /// Returns whether the region may acknowledge a write that is already in the WAL at `now`:
    /// the Leader deadline has not passed. A later DowngradingLeader grant does not fail it.
    pub(crate) fn allows_ack(&self, node: &NodeLease, now: Instant) -> bool {
        self.is_valid(node, now, false)
    }

    fn is_valid(&self, node: &NodeLease, now: Instant, check_role: bool) -> bool {
        if !node.is_enabled() {
            return true;
        }
        let state = self.state.lock().unwrap();
        if check_role && state.role.is_some_and(|role| !role.writable()) {
            return false;
        }
        // A region created on this node has the node lease as its Leader deadline until its
        // first Leader grant.
        let deadline = state.leader_deadline.or_else(|| {
            state
                .created_at
                .and_then(|created_at| node.created_region_deadline(created_at))
        });
        deadline.is_some_and(|deadline| now < deadline)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const LEASE: Duration = Duration::from_secs(10);

    fn enabled_node_lease() -> NodeLease {
        let node = NodeLease::default();
        node.enable();
        node
    }

    fn grant(record: &RegionLeaseRecord, role: RegionRole, heartbeat_sent_at: Instant) -> bool {
        let grant = RegionLeaseGrant {
            role,
            heartbeat_sent_at,
            deadline: heartbeat_sent_at + LEASE,
        };
        record.grant(grant, |_| {})
    }

    #[test]
    fn test_downgrading_grant_keeps_leader_deadline() {
        let node = enabled_node_lease();
        let record = RegionLeaseRecord::default();
        let sent_at = Instant::now();
        assert!(grant(&record, RegionRole::Leader, sent_at));
        // The metasrv keeps renewing the lease as DowngradingLeader after a route is marked as
        // downgrading.
        assert!(grant(
            &record,
            RegionRole::DowngradingLeader,
            sent_at + LEASE / 2
        ));

        let now = sent_at + Duration::from_secs(1);
        assert!(!record.allows_write(&node, now));
        assert!(record.allows_ack(&node, now));
        assert!(record.allows_ack(&node, sent_at + LEASE - Duration::from_millis(1)));
        assert!(!record.allows_ack(&node, sent_at + LEASE));
    }

    #[test]
    fn test_created_region_writes_within_one_lease() {
        let node = enabled_node_lease();
        let created_at = Instant::now();
        let record = RegionLeaseRecord::created(created_at);
        // No heartbeat response has carried region leases yet.
        assert!(!record.allows_write(&node, created_at));

        // The node lease keeps being renewed, but the region uses it only within one lease of
        // its creation.
        node.renew(created_at + LEASE, LEASE);
        assert!(record.allows_write(&node, created_at + LEASE - Duration::from_millis(1)));
        assert!(!record.allows_write(&node, created_at + LEASE));
    }

    #[test]
    fn test_created_region_acknowledges_after_first_downgrading_grant() {
        let node = enabled_node_lease();
        let created_at = Instant::now();
        let record = RegionLeaseRecord::created(created_at);
        node.renew(created_at, LEASE);
        let sent_at = created_at + Duration::from_secs(1);
        assert!(grant(&record, RegionRole::DowngradingLeader, sent_at));

        assert!(!record.allows_write(&node, sent_at));
        assert!(record.allows_ack(&node, sent_at));
        assert!(!record.allows_ack(&node, created_at + LEASE));
    }
}
