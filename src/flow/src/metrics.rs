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
    pub static ref METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME: HistogramVec = register_histogram_vec!(
        "greptime_flow_batching_engine_query_time_secs",
        "flow batching engine query time(seconds)",
        &["flow_id"],
        vec![0.0, 5., 10., 20., 40., 80., 160., 320., 640.,]
    )
    .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_SLOW_QUERY: HistogramVec = register_histogram_vec!(
        "greptime_flow_batching_engine_slow_query_secs",
        "flow batching engine slow query(seconds)",
        &["flow_id", "peer"],
        vec![60., 2. * 60., 3. * 60., 5. * 60., 10. * 60.]
    )
    .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_STALLED_QUERY_WINDOW_CNT: HistogramVec =
        register_histogram_vec!(
            "greptime_flow_batching_engine_stalled_query_window_cnt",
            "flow batching engine stalled query time window count",
            &["flow_id"],
            vec![0.0, 5., 10., 20., 40.]
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_CNT: HistogramVec =
        register_histogram_vec!(
            "greptime_flow_batching_engine_query_window_cnt",
            "flow batching engine query time window count",
            &["flow_id"],
            vec![0.0, 5., 10., 20., 40.]
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_QUERY_TIME_RANGE: HistogramVec =
        register_histogram_vec!(
            "greptime_flow_batching_engine_query_time_range_secs",
            "flow batching engine query time range(seconds)",
            &["flow_id"],
            vec![60., 4. * 60., 16. * 60., 64. * 60., 256. * 60.]
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_BULK_MARK_TIME_WINDOW_RANGE: HistogramVec =
        register_histogram_vec!(
            "greptime_flow_batching_engine_bulk_mark_time_window_range_secs",
            "flow batching engine query time window range marked by bulk memtable in seconds",
            &["flow_id"],
            vec![0.0, 60., 4. * 60., 16. * 60., 64. * 60., 256. * 60.]
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_START_QUERY_CNT: IntCounterVec =
        register_int_counter_vec!(
            "greptime_flow_batching_start_query_count",
            "flow batching engine started query count",
            &["flow_id"],
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_ERROR_CNT: IntCounterVec =
        register_int_counter_vec!(
            "greptime_flow_batching_error_count",
            "flow batching engine error count per flow id",
            &["flow_id"]
        )
        .unwrap();
    pub static ref METRIC_FLOW_BATCHING_ENGINE_GUESS_FE_LOAD: HistogramVec =
        register_histogram_vec!(
            "greptime_flow_batching_engine_guess_fe_load",
            "flow batching engine guessed frontend load",
            &["fe_addr"],
            vec![60., 4. * 60., 16. * 60., 64. * 60., 256. * 60.]
        )
        .unwrap();
    pub static ref METRIC_FLOW_RUN_INTERVAL_MS: IntGauge =
        register_int_gauge!("greptime_flow_run_interval_ms", "flow run interval in ms").unwrap();
    pub static ref METRIC_FLOW_ROWS: IntCounterVec = register_int_counter_vec!(
        "greptime_flow_processed_rows",
        "Count of rows flowing through the system.",
        &["direction"]
    )
    .unwrap();
    pub static ref METRIC_FLOW_PROCESSING_TIME: HistogramVec = register_histogram_vec!(
        "greptime_flow_processing_time",
        "Time spent processing requests",
        &["type"]
    )
    .unwrap();
    pub static ref METRIC_FLOW_ERRORS: IntCounterVec = register_int_counter_vec!(
        "greptime_flow_errors",
        "Count of errors in flow processing",
        &["code"]
    )
    .unwrap();
}
