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

pub const TABLE_TYPE_PHYSICAL: &str = "physical";
pub const TABLE_TYPE_LOGICAL: &str = "logical";
pub const ERROR_TYPE_RETRYABLE: &str = "retryable";
pub const ERROR_TYPE_EXTERNAL: &str = "external";

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
    pub static ref METRIC_META_PROCEDURE_CREATE_VIEW: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_create_view",
        "meta procedure create view",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_CREATE_FLOW: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_create_flow",
        "meta procedure create flow",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_DROP_FLOW: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_drop_flow",
        "meta procedure drop flow",
        &["step"]
    )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_DROP_VIEW: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_drop_view",
        "meta procedure drop view",
        &["step"]
    )
    .unwrap();
    pub static ref METRIC_META_PROCEDURE_CREATE_TABLES: HistogramVec = register_histogram_vec!(
        "greptime_meta_procedure_create_tables",
        "meta procedure create tables",
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
    /// Cache container cache get counter.
    pub static ref CACHE_CONTAINER_CACHE_GET: IntCounterVec = register_int_counter_vec!(
        "greptime_meta_cache_container_cache_get",
        "cache container cache get",
        &["name"]
    )
    .unwrap();
    /// Cache container cache miss counter.
    pub static ref CACHE_CONTAINER_CACHE_MISS: IntCounterVec = register_int_counter_vec!(
        "greptime_meta_cache_container_cache_miss",
        "cache container cache miss",
        &["name"]
    )
    .unwrap();
    /// Cache container load cache timer
    pub static ref CACHE_CONTAINER_LOAD_CACHE: HistogramVec = register_histogram_vec!(
        "greptime_meta_cache_container_load_cache",
        "cache container load cache",
        &["name"]
    )
    .unwrap();
    pub static ref RDS_SQL_EXECUTE_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_meta_rds_pg_sql_execute_elapsed_ms",
        "rds pg sql execute elapsed",
        &["backend", "result", "op", "type"]
    )
    .unwrap();
    pub static ref METRIC_META_RECONCILIATION_NO_REGION_METADATA: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_no_region_metadata",
            "reconciliation no region metadata",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_REGION_NOT_OPEN: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_region_not_open",
            "reconciliation region not open",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_LIST_REGION_METADATA_DURATION: HistogramVec =
        register_histogram_vec!(
            "greptime_meta_reconciliation_list_region_metadata_duration",
            "reconciliation list region metadata duration",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_COLUMN_METADATA_CONSISTENT: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_column_metadata_consistent",
            "reconciliation column metadata consistent",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_COLUMN_METADATA_INCONSISTENT: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_column_metadata_inconsistent",
            "reconciliation column metadata inconsistent",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_RESOLVED_COLUMN_METADATA: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_resolved_column_metadata",
            "reconciliation resolved column metadata",
            &["strategy"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_UPDATE_TABLE_INFO: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_update_table_info",
            "reconciliation update table info",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_RECONCILIATION_CREATE_TABLES: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_reconciliation_create_tables",
            "reconciliation create tables",
            &["table_type"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_TABLE: HistogramVec =
        register_histogram_vec!(
            "greptime_meta_procedure_reconcile_table",
            "reconcile table procedure",
            &["step"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_TABLE_ERROR: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_procedure_reconcile_table_error",
            "reconcile table procedure error",
            &["step", "error_type"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_LOGICAL_TABLES: HistogramVec =
        register_histogram_vec!(
            "greptime_meta_procedure_reconcile_logical_tables",
            "reconcile logical tables procedure",
            &["step"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_LOGICAL_TABLES_ERROR: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_procedure_reconcile_logical_tables_error",
            "reconcile logical tables procedure error",
            &["step", "error_type"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_DATABASE: HistogramVec =
        register_histogram_vec!(
            "greptime_meta_procedure_reconcile_database",
            "reconcile database procedure",
            &["step"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_DATABASE_ERROR: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_procedure_reconcile_database_error",
            "reconcile database procedure error",
            &["step", "error_type"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_CATALOG: HistogramVec =
        register_histogram_vec!(
            "greptime_meta_procedure_reconcile_catalog",
            "reconcile catalog procedure",
            &["step"]
        )
        .unwrap();
    pub static ref METRIC_META_PROCEDURE_RECONCILE_CATALOG_ERROR: IntCounterVec =
        register_int_counter_vec!(
            "greptime_meta_procedure_reconcile_catalog_error",
            "reconcile catalog procedure error",
            &["step", "error_type"]
        )
        .unwrap();
}
