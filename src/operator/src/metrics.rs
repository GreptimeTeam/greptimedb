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
    pub static ref DIST_INGEST_ROW_COUNT: IntCounter = register_int_counter!(
        "greptime_table_operator_ingest_rows",
        "table operator ingest rows"
    )
    .unwrap();
    pub static ref DIST_DELETE_ROW_COUNT: IntCounter = register_int_counter!(
        "greptime_table_operator_delete_rows",
        "table operator delete rows"
    )
    .unwrap();
}
