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

lazy_static! {
    /// Elapsed time to responding kv requests.
    pub static ref METRIC_META_KV_REQUEST_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_meta_kv_request_elapsed",
        "meta kv request",
        &["target", "op"]
    )
    .unwrap();
    /// The heartbeat connection gauge.
    pub static ref METRIC_META_HEARTBEAT_CONNECTION_NUM: IntGauge = register_int_gauge!(
        "greptime_meta_heartbeat_connection_num",
        "meta heartbeat connection num"
    )
    .unwrap();
    /// Elapsed time to execution of heartbeat handlers.
    pub static ref METRIC_META_HANDLER_EXECUTE: HistogramVec =
        register_histogram_vec!("greptime_meta_handler_execute", "meta handler execute", &["name"]).unwrap();
    /// Inactive region gauge.
    pub static ref METRIC_META_INACTIVE_REGIONS: IntGauge =
        register_int_gauge!("greptime_meta_inactive_regions", "meta inactive regions").unwrap();
    /// Elapsed time to leader cache kv.
    pub static ref METRIC_META_LEADER_CACHED_KV_LOAD_ELAPSED: HistogramVec =
        register_histogram_vec!("greptime_meta_leader_cache_kv_load", "meta load cache", &["prefix"])
            .unwrap();
    /// Meta kv cache hit counter.
    pub static ref METRIC_META_KV_CACHE_HIT: IntCounterVec =
        register_int_counter_vec!("greptime_meta_kv_cache_hit", "meta kv cache hit", &["op"]).unwrap();
    /// Meta kv cache miss counter.
    pub static ref METRIC_META_KV_CACHE_MISS: IntCounterVec =
        register_int_counter_vec!("greptime_meta_kv_cache_miss", "meta kv cache miss", &["op"]).unwrap();
    ///  Heartbeat received by metasrv.
    pub static ref METRIC_META_HEARTBEAT_RECV: IntCounterVec =
        register_int_counter_vec!("greptime_meta_heartbeat_recv", "heartbeats received by metasrv", &["pusher_key"]).unwrap();
    /// The migration execute histogram.
    pub static ref METRIC_META_REGION_MIGRATION_EXECUTE: HistogramVec =
        register_histogram_vec!("greptime_meta_region_migration_execute", "meta region migration execute", &["state"]).unwrap();
    /// The migration error counter.
    pub static ref METRIC_META_REGION_MIGRATION_ERROR: IntCounterVec =
        register_int_counter_vec!("greptime_meta_region_migration_error", "meta region migration abort", &["state", "error_type"]).unwrap();
    /// The migration datanode counter.
    pub static ref METRIC_META_REGION_MIGRATION_DATANODES: IntCounterVec =
        register_int_counter_vec!("greptime_meta_region_migration_stat", "meta region migration stat", &["datanode_type", "datanode_id"]).unwrap();
    /// The migration fail counter.
    pub static ref METRIC_META_REGION_MIGRATION_FAIL: IntCounter =
        register_int_counter!("greptime_meta_region_migration_fail", "meta region migration fail").unwrap();
    /// The add region follower execute histogram.
    pub static ref METRIC_META_ADD_REGION_FOLLOWER_EXECUTE: HistogramVec =
        register_histogram_vec!("greptime_meta_add_region_follower_execute", "meta add region follower execute", &["state"]).unwrap();
    /// The remove region follower execute histogram.
    pub static ref METRIC_META_REMOVE_REGION_FOLLOWER_EXECUTE: HistogramVec =
        register_histogram_vec!("greptime_meta_remove_region_follower_execute", "meta remove region follower execute", &["state"]).unwrap();
}
