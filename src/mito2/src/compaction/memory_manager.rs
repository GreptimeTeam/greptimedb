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

use common_memory_manager::{MemoryGuard, MemoryManager, MemoryMetrics};

use crate::metrics::{
    COMPACTION_MEMORY_IN_USE, COMPACTION_MEMORY_LIMIT, COMPACTION_MEMORY_REJECTED,
};

/// Compaction-specific memory metrics implementation.
#[derive(Clone, Copy, Debug, Default)]
pub struct CompactionMemoryMetrics;

impl MemoryMetrics for CompactionMemoryMetrics {
    fn set_limit(&self, bytes: i64) {
        COMPACTION_MEMORY_LIMIT.set(bytes);
    }

    fn set_in_use(&self, bytes: i64) {
        COMPACTION_MEMORY_IN_USE.set(bytes);
    }

    fn inc_rejected(&self, reason: &str) {
        COMPACTION_MEMORY_REJECTED
            .with_label_values(&[reason])
            .inc();
    }
}

/// Compaction memory manager.
pub type CompactionMemoryManager = MemoryManager<CompactionMemoryMetrics>;

/// Compaction memory guard.
pub type CompactionMemoryGuard = MemoryGuard<CompactionMemoryMetrics>;

/// Helper to construct a compaction memory manager without passing metrics explicitly.
pub fn new_compaction_memory_manager(limit_bytes: u64) -> CompactionMemoryManager {
    CompactionMemoryManager::new(limit_bytes, CompactionMemoryMetrics)
}
