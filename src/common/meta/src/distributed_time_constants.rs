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

use etcd_client::ConnectOptions;

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

pub const FLOWNODE_LEASE_SECS: u64 = DATANODE_LEASE_SECS;

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

/// The default options for the etcd client.
pub fn default_etcd_client_options() -> ConnectOptions {
    ConnectOptions::new()
        .with_keep_alive_while_idle(true)
        .with_keep_alive(
            Duration::from_secs(META_KEEP_ALIVE_INTERVAL_SECS + 1),
            Duration::from_secs(10),
        )
        .with_connect_timeout(Duration::from_secs(10))
}

/// The default mailbox round-trip timeout.
pub const MAILBOX_RTT_SECS: u64 = 1;

/// The interval of reporting topic stats.
pub const TOPIC_STATS_REPORT_INTERVAL_SECS: u64 = 15;

/// The retention seconds of topic stats.
pub const TOPIC_STATS_RETENTION_SECS: u64 = TOPIC_STATS_REPORT_INTERVAL_SECS * 100;
