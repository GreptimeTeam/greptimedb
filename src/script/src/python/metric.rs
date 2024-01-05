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
    pub static ref METRIC_RSPY_INIT_ELAPSED: Histogram = register_histogram!(
        "greptime_script_rspy_init_elapsed",
        "script rspy init elapsed"
    )
    .unwrap();
    pub static ref METRIC_RSPY_EXEC_ELAPSED: Histogram = register_histogram!(
        "greptime_script_rspy_exec_elapsed",
        "script rspy exec elapsed"
    )
    .unwrap();
    pub static ref METRIC_RSPY_EXEC_TOTAL_ELAPSED: Histogram = register_histogram!(
        "greptime_script_rspy_exec_total_elapsed",
        "script rspy exec total elapsed"
    )
    .unwrap();
}

#[cfg(feature = "pyo3_backend")]
lazy_static! {
    pub static ref METRIC_PYO3_EXEC_ELAPSED: Histogram = register_histogram!(
        "greptime_script_pyo3_exec_elapsed",
        "script pyo3 exec elapsed"
    )
    .unwrap();
    pub static ref METRIC_PYO3_INIT_ELAPSED: Histogram = register_histogram!(
        "greptime_script_pyo3_init_elapsed",
        "script pyo3 init elapsed"
    )
    .unwrap();
    pub static ref METRIC_PYO3_EXEC_TOTAL_ELAPSED: Histogram = register_histogram!(
        "greptime_script_pyo3_exec_total_elapsed",
        "script pyo3 exec total elapsed"
    )
    .unwrap();
}
