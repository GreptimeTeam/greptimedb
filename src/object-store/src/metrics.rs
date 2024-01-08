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

//! object-store metrics

/// Cache hit counter, no matter what the cache result is.
use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    /// Cache hit counter, no matter what the cache result is.
    pub static ref OBJECT_STORE_LRU_CACHE_HIT: IntCounterVec = register_int_counter_vec!(
        "greptime_object_store_lru_cache_hit",
        "object store lru cache hit",
        &["result"]
    )
    .unwrap();
    /// Cache miss counter
    pub static ref OBJECT_STORE_LRU_CACHE_MISS: IntCounter =
        register_int_counter!("greptime_object_store_lru_cache_miss", "object store lru cache miss")
            .unwrap();
    /// Object store read error counter
    pub static ref OBJECT_STORE_READ_ERROR: IntCounterVec = register_int_counter_vec!(
        "greptime_object_store_read_errors",
        "object store read errors",
        &["kind"]
    )
    .unwrap();

    /// Cache entry number
    pub static ref OBJECT_STORE_LRU_CACHE_ENTRIES: IntGauge =
        register_int_gauge!("greptime_object_store_lru_cache_entries", "object store lru cache entries")
            .unwrap();

    /// Cache size in bytes
    pub static ref OBJECT_STORE_LRU_CACHE_BYTES: IntGauge =
        register_int_gauge!("greptime_object_store_lru_cache_bytes",  "object store lru cache bytes")
            .unwrap();
}
