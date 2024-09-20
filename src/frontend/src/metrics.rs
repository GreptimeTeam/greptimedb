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
    /// Timer of handling query in RPC handler.
    pub static ref GRPC_HANDLE_QUERY_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_frontend_grpc_handle_query_elapsed",
        "Elapsed time of handling queries in RPC handler",
        &["type"],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref GRPC_HANDLE_SQL_ELAPSED: Histogram = GRPC_HANDLE_QUERY_ELAPSED
        .with_label_values(&["sql"]);
    pub static ref GRPC_HANDLE_PROMQL_ELAPSED: Histogram = GRPC_HANDLE_QUERY_ELAPSED
        .with_label_values(&["promql"]);

    /// Timer of handling scripts in the script handler.
    pub static ref HANDLE_SCRIPT_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_frontend_handle_script_elapsed",
        "Elapsed time of handling scripts in the script handler",
        &["type"],
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
    pub static ref INSERT_SCRIPTS_ELAPSED: Histogram = HANDLE_SCRIPT_ELAPSED
        .with_label_values(&["insert"]);
    pub static ref EXECUTE_SCRIPT_ELAPSED: Histogram = HANDLE_SCRIPT_ELAPSED
        .with_label_values(&["execute"]);

    /// The number of OpenTelemetry metrics send by frontend node.
    pub static ref OTLP_METRICS_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_metrics_rows",
        "frontend otlp metrics rows"
    )
    .unwrap();

    /// The number of OpenTelemetry traces send by frontend node.
    pub static ref OTLP_TRACES_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_traces_rows",
        "frontend otlp traces rows"
    )
    .unwrap();

    /// The number of OpenTelemetry logs send by frontend node.
    pub static ref OTLP_LOGS_ROWS: IntCounter = register_int_counter!(
        "greptime_frontend_otlp_logs_rows",
        "frontend otlp logs rows"
    )
    .unwrap();

    /// The number of heartbeats send by frontend node.
    pub static ref HEARTBEAT_SENT_COUNT: IntCounter = register_int_counter!(
        "greptime_frontend_heartbeat_send_count",
        "frontend heartbeat sent",
    )
    .unwrap();
    /// The number of heartbeats received by frontend node, labeled with result type.
    pub static ref HEARTBEAT_RECV_COUNT: IntCounterVec = register_int_counter_vec!(
        "greptime_frontend_heartbeat_recv_count",
        "frontend heartbeat received",
        &["result"]
    )
    .unwrap();
}
