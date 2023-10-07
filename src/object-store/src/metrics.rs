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
pub const OBJECT_STORE_LRU_CACHE_HIT: &str = "object_store.lru_cache.hit";
/// Cache miss counter
pub const OBJECT_STORE_LRU_CACHE_MISS: &str = "object_store.lru_cache.miss";
/// Object store read error counter
pub const OBJECT_STORE_READ_ERROR: &str = "object_store.read.errors";
/// Cache entry number
pub const OBJECT_STORE_LRU_CACHE_ENTRIES: &str = "object_store.lru_cache.entries";
/// Cache size in bytes
pub const OBJECT_STORE_LRU_CACHE_BYTES: &str = "object_store.lru_cache.bytes";
