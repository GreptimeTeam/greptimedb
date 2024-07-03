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

use mockall::lazy_static;
use prometheus::{register_int_counter_vec, IntCounterVec};

lazy_static! {
    pub static ref INDEX_CACHE_STATS: IntCounterVec = register_int_counter_vec!(
        "greptime_index_cache",
        "greptime index cache stats",
        &["cache_type", "op_type"]
    )
    .unwrap();
}
