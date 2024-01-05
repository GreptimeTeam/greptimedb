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

use lazy_static::lazy_static;
use prometheus::*;

/// Region request type label.
pub const REGION_REQUEST_TYPE: &str = "datanode_region_request_type";

pub const REGION_ROLE: &str = "region_role";
pub const REGION_ID: &str = "region_id";

lazy_static! {
    /// The elapsed time of handling a request in the region_server.
    pub static ref HANDLE_REGION_REQUEST_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_datanode_handle_region_request_elapsed",
        "datanode handle region request elapsed",
        &[REGION_REQUEST_TYPE]
    )
    .unwrap();
    /// The elapsed time since the last received heartbeat.
    pub static ref LAST_RECEIVED_HEARTBEAT_ELAPSED: IntGauge = register_int_gauge!(
        "greptime_last_received_heartbeat_lease_elapsed",
        "last received heartbeat lease elapsed",
    )
    .unwrap();
    pub static ref LEASE_EXPIRED_REGION: IntGaugeVec = register_int_gauge_vec!(
        "greptime_lease_expired_region",
        "lease expired region",
        &[REGION_ID]
    )
    .unwrap();
    /// The received region leases via heartbeat.
    pub static ref HEARTBEAT_REGION_LEASES: IntGaugeVec = register_int_gauge_vec!(
        "greptime_heartbeat_region_leases",
        "received region leases via heartbeat",
        &[REGION_ROLE]
    )
    .unwrap();
}
