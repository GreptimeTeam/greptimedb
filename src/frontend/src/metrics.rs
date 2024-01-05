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
    pub static ref METRIC_HANDLE_SQL_ELAPSED: Histogram =
        register_histogram!("greptime_frontend_handle_sql_elapsed", "frontend handle sql elapsed").unwrap();
    pub static ref METRIC_HANDLE_PROMQL_ELAPSED: Histogram = register_histogram!(
        "greptime_frontend_handle_promql_elapsed",
        "frontend handle promql elapsed"
    )
    .unwrap();
    pub static ref METRIC_EXEC_PLAN_ELAPSED: Histogram =
        register_histogram!("greptime_frontend_exec_plan_elapsed", "frontend exec plan elapsed").unwrap();
    pub static ref METRIC_HANDLE_SCRIPTS_ELAPSED: Histogram = register_histogram!(
        "greptime_frontend_handle_scripts_elapsed",
        "frontend handle scripts elapsed"
    )
    .unwrap();
    pub static ref METRIC_RUN_SCRIPT_ELAPSED: Histogram =
        register_histogram!("greptime_frontend_run_script_elapsed", "frontend run script elapsed").unwrap();
    /// The samples count of Prometheus remote write.
    pub static ref PROM_STORE_REMOTE_WRITE_SAMPLES: IntCounter = register_int_counter!(
        "greptime_frontend_prometheus_remote_write_samples",
        "frontend prometheus remote write samples"
    )
    .unwrap();
    pub static ref OTLP_METRICS_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_metrics_rows",
        "frontend otlp metrics rows"
    )
    .unwrap();
    pub static ref OTLP_TRACES_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_traces_rows",
        "frontend otlp traces rows"
    )
    .unwrap();
}
