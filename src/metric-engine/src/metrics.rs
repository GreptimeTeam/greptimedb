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

//! Internal metrics for observability.

use lazy_static::lazy_static;
use prometheus::*;

/// Stage label.
pub const OPERATION_LABEL: &str = "operation";

lazy_static! {
    /// Gauge for opened regions
    pub static ref PHYSICAL_REGION_COUNT: IntGauge =
        register_int_gauge!("greptime_metric_engine_physical_region_count", "metric engine physical region count").unwrap();

    /// Gauge of columns across all opened regions
    pub static ref PHYSICAL_COLUMN_COUNT: IntGauge =
        register_int_gauge!("greptime_metric_engine_physical_column_count", "metric engine physical column count").unwrap();

    /// Gauge for opened logical regions
    pub static ref LOGICAL_REGION_COUNT: IntGauge =
        register_int_gauge!("greptime_metric_engine_logical_region_count", "metric engine logical region count").unwrap();

    /// Histogram for opened logical regions
    pub static ref MITO_DDL_DURATION: Histogram =
        register_histogram!("greptime_metric_engine_mito_ddl", "metric engine mito ddl").unwrap();

    /// Counter for forbidden operations
    pub static ref FORBIDDEN_OPERATION_COUNT: IntCounter =
        register_int_counter!("greptime_metric_engine_forbidden_request", "metric forbidden request").unwrap();

    /// Histogram for underlying mito operations
    pub static ref MITO_OPERATION_ELAPSED: HistogramVec = register_histogram_vec!(
        "greptime_metric_engine_mito_op_elapsed",
        "metric engine's mito operation elapsed",
        &[OPERATION_LABEL],
        // 0.01 ~ 10000
        exponential_buckets(0.01, 10.0, 7).unwrap(),
    )
    .unwrap();
}
