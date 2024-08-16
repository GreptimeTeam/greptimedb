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

//! Some of the metrics used in the flow module.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref METRIC_FLOW_TASK_COUNT: IntGauge =
        register_int_gauge!("greptime_flow_task_count", "flow task count").unwrap();
    pub static ref METRIC_FLOW_INPUT_BUF_SIZE: IntGauge =
        register_int_gauge!("greptime_flow_input_buf_size", "flow input buf size").unwrap();
    pub static ref METRIC_FLOW_INSERT_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_flow_insert_elapsed",
        "flow insert elapsed",
        &["table_id"]
    )
    .unwrap();
    pub static ref METRIC_FLOW_RUN_INTERVAL_MS: IntGauge =
        register_int_gauge!("greptime_flow_run_interval_ms", "flow run interval in ms").unwrap();
}
