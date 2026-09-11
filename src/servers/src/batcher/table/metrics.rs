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
use prometheus::{
    Histogram, IntCounter, IntGauge, register_histogram, register_int_counter, register_int_gauge,
};

lazy_static! {
    pub(in crate::batcher::table) static ref PENDING_WORKERS: IntGauge = register_int_gauge!(
        "greptime_table_batcher_pending_workers",
        "Registered table batch workers"
    )
    .unwrap();
    pub(in crate::batcher::table) static ref FLUSH_DROPPED_ROWS: IntCounter =
        register_int_counter!(
            "greptime_table_batcher_flush_dropped_rows",
            "Rows in failed table batch flushes"
        )
        .unwrap();
    pub(in crate::batcher::table) static ref PENDING_BATCHES: IntGauge = register_int_gauge!(
        "greptime_table_batcher_pending_batches",
        "Pending table batches"
    )
    .unwrap();
    pub(in crate::batcher::table) static ref PENDING_ROWS: IntGauge = register_int_gauge!(
        "greptime_table_batcher_pending_rows",
        "Rows awaiting table batch flush"
    )
    .unwrap();
    pub(in crate::batcher::table) static ref FLUSH_TOTAL: IntCounter = register_int_counter!(
        "greptime_table_batcher_flush_total",
        "Successful table batch flushes"
    )
    .unwrap();
    pub(in crate::batcher::table) static ref FLUSH_FAILURES: IntCounter = register_int_counter!(
        "greptime_table_batcher_flush_failures",
        "Failed table batch flushes"
    )
    .unwrap();
    pub(in crate::batcher::table) static ref FLUSH_ROWS: Histogram = register_histogram!(
        "greptime_table_batcher_flush_rows",
        "Number of rows per successful table batch flush",
        vec![100.0, 1000.0, 10000.0, 50000.0, 100000.0, 500000.0]
    )
    .unwrap();
    pub(in crate::batcher::table) static ref FLUSH_ELAPSED: Histogram = register_histogram!(
        "greptime_table_batcher_flush_elapsed",
        "Table batch flush duration in seconds",
        vec![0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 60.0, 300.0]
    )
    .unwrap();
}
