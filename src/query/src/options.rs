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

use common_base::memory_limit::MemoryLimit;
use serde::{Deserialize, Serialize};

/// Query engine config
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct QueryOptions {
    /// Parallelism of query engine. Default to 0, which implies the number of logical CPUs.
    pub parallelism: usize,
    /// Whether to allow query fallback when push down fails.
    pub allow_query_fallback: bool,
    /// Memory pool size for query execution. Setting it to 0 disables the limit (unbounded).
    /// Supports absolute size (e.g., "2GB") or percentage (e.g., "50%").
    /// When this limit is reached, queries will fail with ResourceExhausted error.
    pub memory_pool_size: MemoryLimit,
}

#[allow(clippy::derivable_impls)]
impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            parallelism: 0,
            allow_query_fallback: false,
            memory_pool_size: MemoryLimit::default(),
        }
    }
}
