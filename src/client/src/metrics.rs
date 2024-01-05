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
    pub static ref METRIC_GRPC_CREATE_TABLE: Histogram =
        register_histogram!("greptime_grpc_create_table", "grpc create table").unwrap();
    pub static ref METRIC_GRPC_PROMQL_RANGE_QUERY: Histogram = register_histogram!(
        "greptime_grpc_promql_range_query",
        "grpc promql range query"
    )
    .unwrap();
    pub static ref METRIC_GRPC_INSERT: Histogram =
        register_histogram!("greptime_grpc_insert", "grpc insert").unwrap();
    pub static ref METRIC_GRPC_DELETE: Histogram =
        register_histogram!("greptime_grpc_delete", "grpc delete").unwrap();
    pub static ref METRIC_GRPC_SQL: Histogram =
        register_histogram!("greptime_grpc_sql", "grpc sql").unwrap();
    pub static ref METRIC_GRPC_LOGICAL_PLAN: Histogram =
        register_histogram!("greptime_grpc_logical_plan", "grpc logical plan").unwrap();
    pub static ref METRIC_GRPC_ALTER: Histogram =
        register_histogram!("greptime_grpc_alter", "grpc alter").unwrap();
    pub static ref METRIC_GRPC_DROP_TABLE: Histogram =
        register_histogram!("greptime_grpc_drop_table", "grpc drop table").unwrap();
    pub static ref METRIC_GRPC_TRUNCATE_TABLE: Histogram =
        register_histogram!("greptime_grpc_truncate_table", "grpc truncate table").unwrap();
    pub static ref METRIC_GRPC_DO_GET: Histogram =
        register_histogram!("greptime_grpc_do_get", "grpc do get").unwrap();
    pub static ref METRIC_REGION_REQUEST_GRPC: HistogramVec = register_histogram_vec!(
        "greptime_grpc_region_request",
        "grpc region request",
        &["request_type"]
    )
    .unwrap();
}
