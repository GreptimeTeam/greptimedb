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
    pub static ref METRIC_CATALOG_MANAGER_CATALOG_COUNT: IntGauge =
        register_int_gauge!("greptime_catalog_catalog_count", "catalog catalog count").unwrap();
    pub static ref METRIC_CATALOG_MANAGER_SCHEMA_COUNT: IntGauge =
        register_int_gauge!("greptime_catalog_schema_count", "catalog schema count").unwrap();
    pub static ref METRIC_CATALOG_MANAGER_TABLE_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "greptime_catalog_table_count",
        "catalog table count",
        &["db"]
    )
    .unwrap();
    pub static ref METRIC_CATALOG_KV_REMOTE_GET: Histogram =
        register_histogram!("greptime_catalog_kv_get_remote", "catalog kv get remote").unwrap();
    pub static ref METRIC_CATALOG_KV_GET: Histogram =
        register_histogram!("greptime_catalog_kv_get", "catalog kv get").unwrap();
    pub static ref METRIC_CATALOG_KV_BATCH_GET: Histogram =
        register_histogram!("greptime_catalog_kv_batch_get", "catalog kv batch get").unwrap();

    /// Count of running process in each catalog.
    pub static ref PROCESS_LIST_COUNT: IntGaugeVec = register_int_gauge_vec!(
        "greptime_process_list_count",
        "Running process count per catalog",
        &["catalog"]
    )
    .unwrap();

    /// Count of killed process in each catalog.
    pub static ref PROCESS_KILL_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_process_kill_count",
        "Completed kill process requests count",
        &["catalog"]
    )
    .unwrap();
}
