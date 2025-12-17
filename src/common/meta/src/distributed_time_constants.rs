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

use std::sync::OnceLock;
use std::time::Duration;

pub const BASE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(3);

/// The lease seconds of metasrv leader.
pub const META_LEASE_SECS: u64 = 5;

/// The keep-alive interval of the Postgres connection.
pub const POSTGRES_KEEP_ALIVE_SECS: u64 = 30;

/// In a lease, there are two opportunities for renewal.
pub const META_KEEP_ALIVE_INTERVAL_SECS: u64 = META_LEASE_SECS / 2;

/// The timeout of the heartbeat request.
pub const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1);

/// The keep-alive interval of the heartbeat channel.
pub const HEARTBEAT_CHANNEL_KEEP_ALIVE_INTERVAL_SECS: Duration =
    Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1);

/// The keep-alive timeout of the heartbeat channel.
pub const HEARTBEAT_CHANNEL_KEEP_ALIVE_TIMEOUT_SECS: Duration =
    Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1);

/// The default mailbox round-trip timeout.
pub const MAILBOX_RTT_SECS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// The distributed time constants.
pub struct DistributedTimeConstants {
    pub heartbeat_interval: Duration,
    pub frontend_heartbeat_interval: Duration,
    pub region_lease: Duration,
    pub datanode_lease: Duration,
    pub flownode_lease: Duration,
}

/// The frontend heartbeat interval is 6 times of the base heartbeat interval.
pub fn frontend_heartbeat_interval(base_heartbeat_interval: Duration) -> Duration {
    base_heartbeat_interval * 6
}

impl DistributedTimeConstants {
    /// Create a new DistributedTimeConstants from the heartbeat interval.
    pub fn from_heartbeat_interval(heartbeat_interval: Duration) -> Self {
        let region_lease = heartbeat_interval * 3 + Duration::from_secs(1);
        let datanode_lease = region_lease;
        let flownode_lease = datanode_lease;
        Self {
            heartbeat_interval,
            frontend_heartbeat_interval: frontend_heartbeat_interval(heartbeat_interval),
            region_lease,
            datanode_lease,
            flownode_lease,
        }
    }
}

impl Default for DistributedTimeConstants {
    fn default() -> Self {
        Self::from_heartbeat_interval(BASE_HEARTBEAT_INTERVAL)
    }
}

static DEFAULT_DISTRIBUTED_TIME_CONSTANTS: OnceLock<DistributedTimeConstants> = OnceLock::new();

/// Get the default distributed time constants.
pub fn default_distributed_time_constants() -> &'static DistributedTimeConstants {
    DEFAULT_DISTRIBUTED_TIME_CONSTANTS.get_or_init(Default::default)
}

/// Initialize the default distributed time constants.
pub fn init_distributed_time_constants(base_heartbeat_interval: Duration) {
    let distributed_time_constants =
        DistributedTimeConstants::from_heartbeat_interval(base_heartbeat_interval);
    DEFAULT_DISTRIBUTED_TIME_CONSTANTS
        .set(distributed_time_constants)
        .expect("Failed to set default distributed time constants");
    common_telemetry::info!(
        "Initialized default distributed time constants: {:#?}",
        distributed_time_constants
    );
}
