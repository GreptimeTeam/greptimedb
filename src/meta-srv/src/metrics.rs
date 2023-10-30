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
    pub static ref METRIC_META_KV_REQUEST: HistogramVec = register_histogram_vec!(
        "meta_kv_request",
        "meta kv request",
        &["target", "op", "cluster_id"]
    )
    .unwrap();
    pub static ref METRIC_META_ROUTE_REQUEST: HistogramVec = register_histogram_vec!(
        "meta_route_request",
        "meta route request",
        &["op", "cluster_id"]
    )
    .unwrap();
    pub static ref METRIC_META_HEARTBEAT_CONNECTION_NUM: IntGauge = register_int_gauge!(
        "meta_heartbeat_connection_num",
        "meta heartbeat connection num"
    )
    .unwrap();
    pub static ref METRIC_META_HANDLER_EXECUTE: HistogramVec =
        register_histogram_vec!("meta_handler_execute", "meta handler execute", &["name"]).unwrap();
    pub static ref METRIC_META_INACTIVE_REGIONS: IntGauge =
        register_int_gauge!("meta_inactive_regions", "meta inactive regions").unwrap();
}
