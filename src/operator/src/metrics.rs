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
    pub static ref DIST_CREATE_TABLE: Histogram = register_histogram!(
        "greptime_table_operator_create_table",
        "table operator create table"
    )
    .unwrap();
    pub static ref DIST_CREATE_TABLES: Histogram = register_histogram!(
        "greptime_table_operator_create_tables",
        "table operator create table"
    )
    .unwrap();
    pub static ref DIST_ALTER_TABLES: Histogram = register_histogram!(
        "greptime_table_operator_alter_tables",
        "table operator alter table"
    )
    .unwrap();
    pub static ref DIST_INGEST_ROW_COUNT: IntCounter = register_int_counter!(
        "greptime_table_operator_ingest_rows",
        "table operator ingest rows"
    )
    .unwrap();
    pub static ref DIST_MIRROR_ROW_COUNT: IntCounter = register_int_counter!(
        "greptime_table_operator_mirror_rows",
        "table operator mirror rows"
    )
    .unwrap();
    pub static ref DIST_MIRROR_PENDING_ROW_COUNT: IntGauge = register_int_gauge!(
        "greptime_table_operator_mirror_pending_rows",
        "table operator mirror pending rows"
    )
    .unwrap();
    pub static ref DIST_DELETE_ROW_COUNT: IntCounter = register_int_counter!(
        "greptime_table_operator_delete_rows",
        "table operator delete rows"
    )
    .unwrap();
    pub static ref DIST_CREATE_VIEW: Histogram = register_histogram!(
        "greptime_ddl_operator_create_view",
        "DDL operator create view"
    )
    .unwrap();
    pub static ref CREATE_ALTER_ON_DEMAND: HistogramVec = register_histogram_vec!(
        "greptime_table_operator_create_alter_on_demand",
        "table operator duration to create or alter tables on demand",
        &["table_type"]
    )
    .unwrap();
    pub static ref HANDLE_BULK_INSERT_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_table_operator_handle_bulk_insert",
        "table operator duration to handle bulk inserts",
        &["stage"],
        vec![
            0.001, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.10, 0.15, 0.2, 0.3, 0.4, 0.5, 1.0, 1.5,
            2.0, 2.5, 3.0, 4.0, 5.0
        ]
    )
    .unwrap();
    pub static ref BULK_REQUEST_MESSAGE_SIZE: Histogram = register_histogram!(
        "greptime_table_operator_bulk_insert_message_size",
        "table operator bulk inserts message encoded size",
        vec![
            32768.0,
            65536.0,
            131072.0,
            262144.0,
            524288.0,
            1048576.0,
            2097152.0,
            4194304.0,
            8388608.0,
            16777216.0,
            33554432.0,
            67108864.0,
            134217728.0,
            268435456.0
        ]
    )
    .unwrap();
    pub static ref BULK_REQUEST_ROWS: HistogramVec = register_histogram_vec!(
        "greptime_table_operator_bulk_insert_message_rows",
        "table operator bulk inserts message rows",
        &["type"],
        // 10 ~ 100_000
        exponential_buckets(10.0, 10.0, 5).unwrap()
    )
    .unwrap();
}
