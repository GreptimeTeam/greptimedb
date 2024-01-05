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

//! Runtime metrics
use lazy_static::lazy_static;
use prometheus::*;

pub const THREAD_NAME_LABEL: &str = "thread_name";

lazy_static! {
    pub static ref METRIC_RUNTIME_THREADS_ALIVE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_runtime_threads_alive",
        "runtime threads alive",
        &[THREAD_NAME_LABEL]
    )
    .unwrap();
    pub static ref METRIC_RUNTIME_THREADS_IDLE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_runtime_threads_idle",
        "runtime threads idle",
        &[THREAD_NAME_LABEL]
    )
    .unwrap();
}
