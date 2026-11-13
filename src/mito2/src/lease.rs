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
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use store_api::storage::RegionId;

/// The instant lease deadlines count from, so that a deadline fits in an [AtomicU64].
static LEASE_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

pub(crate) fn lease_nanos(instant: Instant) -> u64 {
    instant.saturating_duration_since(*LEASE_EPOCH).as_nanos() as u64
}

/// A lease deadline in nanoseconds since [LEASE_EPOCH]. It only moves forward; 0 means none.
#[derive(Debug, Default)]
pub(crate) struct Deadline(AtomicU64);

impl Deadline {
    pub(crate) fn extend(&self, deadline: Instant) {
        self.0.fetch_max(lease_nanos(deadline), Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// Lease of this node: the latest deadline of the heartbeat responses that carry region leases.
#[derive(Debug, Default)]
pub(crate) struct NodeLease {
    /// Whether writes need a lease. Only a datanode the metasrv controls sets it.
    enabled: AtomicBool,
    deadline: Deadline,
    /// The time of the lease checks while a test freezes it, in the unit of [Deadline]; 0 when
    /// it is not frozen.
    #[cfg(any(test, feature = "test"))]
    frozen_now: AtomicU64,
}

impl NodeLease {
    pub(crate) fn enable(&self) {
        self.enabled.store(true, Ordering::Relaxed);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub(crate) fn renew(&self, deadline: Instant) {
        self.deadline.extend(deadline);
    }

    /// Returns the current time in the unit of [Deadline].
    pub(crate) fn now(&self) -> u64 {
        #[cfg(any(test, feature = "test"))]
        {
            let frozen_now = self.frozen_now.load(Ordering::Relaxed);
            if frozen_now != 0 {
                return frozen_now;
            }
        }
        lease_nanos(Instant::now())
    }

    #[cfg(any(test, feature = "test"))]
    pub(crate) fn freeze_clock(&self, at: Instant) {
        self.frozen_now
            .store(lease_nanos(at).max(1), Ordering::Relaxed);
    }

    /// Lets a region created or upgraded on this node write until the node's deadline. The
    /// metasrv granted that deadline before the creation or upgrade, so a later migration of the
    /// region waits past it. Later responses do not extend it; the region's Leader grants do.
    pub(crate) fn seed(&self, leader_deadline: &Deadline) {
        leader_deadline
            .0
            .fetch_max(self.deadline.get(), Ordering::Relaxed);
    }
}

/// A region lease the metasrv granted for a heartbeat sent at `issued_at`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GrantedLease {
    pub(crate) candidate_lease_id: Option<u64>,
    pub(crate) issued_at: Instant,
}

/// The lease of the latest heartbeat each region received, whether or not the region applied it.
/// It is kept apart from the region instance, so a reopen during catchup keeps it.
#[derive(Debug, Default)]
pub(crate) struct GrantedLeases(Mutex<HashMap<RegionId, GrantedLease>>);

impl GrantedLeases {
    pub(crate) fn record(
        &self,
        region_id: RegionId,
        candidate_lease_id: Option<u64>,
        issued_at: Instant,
    ) {
        let lease = GrantedLease {
            candidate_lease_id,
            issued_at,
        };
        self.0
            .lock()
            .unwrap()
            .entry(region_id)
            .and_modify(|latest| {
                if issued_at > latest.issued_at {
                    *latest = lease;
                }
            })
            .or_insert(lease);
    }

    pub(crate) fn remove(&self, region_id: RegionId) {
        self.0.lock().unwrap().remove(&region_id);
    }

    #[cfg(any(test, feature = "test"))]
    pub(crate) fn get(&self, region_id: RegionId) -> Option<GrantedLease> {
        self.0.lock().unwrap().get(&region_id).copied()
    }
}
