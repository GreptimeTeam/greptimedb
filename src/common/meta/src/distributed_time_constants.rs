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

use std::time::Duration;

/// Heartbeat interval time (is the basic unit of various time).
pub const HEARTBEAT_INTERVAL_MILLIS: u64 = 3000;

/// The frontend will also send heartbeats to Metasrv, sending an empty
/// heartbeat every HEARTBEAT_INTERVAL_MILLIS * 6 seconds.
pub const FRONTEND_HEARTBEAT_INTERVAL_MILLIS: u64 = HEARTBEAT_INTERVAL_MILLIS * 6;

/// The lease seconds of a region. It's set by 3 heartbeat intervals
/// (HEARTBEAT_INTERVAL_MILLIS × 3), plus some extra buffer (1 second).
pub const REGION_LEASE_SECS: u64 =
    Duration::from_millis(HEARTBEAT_INTERVAL_MILLIS * 3).as_secs() + 1;

/// When creating table or region failover, a target node needs to be selected.
/// If the node's lease has expired, the `Selector` will not select it.
pub const DATANODE_LEASE_SECS: u64 = REGION_LEASE_SECS;

/// The lease seconds of metasrv leader.
pub const META_LEASE_SECS: u64 = 3;

/// In a lease, there are two opportunities for renewal.
pub const META_KEEP_ALIVE_INTERVAL_SECS: u64 = META_LEASE_SECS / 2;

/// The default mailbox round-trip timeout.
pub const MAILBOX_RTT_SECS: u64 = 1;
