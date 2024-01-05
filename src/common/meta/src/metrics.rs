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
    pub static ref METRIC_META_TXN_REQUEST: HistogramVec = register_histogram_vec!(
        "greptime_meta_txn_request",
        "meta txn request",
        &["target", "op"]
    )
    .unwrap();
    pub static ref METRIC_META_CREATE_CATALOG: Histogram =
        register_histogram!("greptime_meta_create_catalog", "meta create catalog").unwrap();
    pub static ref METRIC_META_CREATE_CATALOG_COUNTER: IntCounter = register_int_counter!(
        "greptime_meta_create_catalog_counter",
        "meta create catalog"
    )
    .unwrap();
    pub static ref METRIC_META_CREATE_SCHEMA: Histogram =
        register_histogram!("greptime_meta_create_schema", "meta create schema").unwrap();
    pub static ref METRIC_META_CREATE_SCHEMA_COUNTER: IntCounter =
        register_int_counter!("greptime_meta_create_schema_counter", "meta create schema").unwrap();
    pub static ref METRIC_META_PROCEDURE_CREATE_TABLE: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_create_table",
        "meta procedure create table",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_DROP_TABLE: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_drop_table",
        "meta procedure drop table",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_ALTER_TABLE: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_alter_table",
        "meta procedure alter table",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_TRUNCATE_TABLE: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_truncate_table",
        "meta procedure truncate table",
        &["step"]
    )
    .unwrap();
}
