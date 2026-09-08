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

//! Estimated SST reader memory, shared across queries independently of runtime tracking.

use common_base::memory_limit::MemoryLimit;
use common_memory_manager::{MemoryGuard, MemoryManager, MemoryMetrics, PermitGranularity};
use futures::{Stream, StreamExt};

use crate::error::{Result, ScanMemoryExhaustedSnafu};
use crate::metrics::{
    SCAN_ESTIMATED_MEMORY_BYTES, SCAN_ESTIMATED_MEMORY_LIMIT_BYTES,
    SCAN_ESTIMATED_MEMORY_REJECTED_TOTAL,
};

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ScanMemoryMetrics;

impl MemoryMetrics for ScanMemoryMetrics {
    fn set_limit(&self, bytes: i64) {
        SCAN_ESTIMATED_MEMORY_LIMIT_BYTES.set(bytes);
    }

    fn set_in_use(&self, bytes: i64) {
        SCAN_ESTIMATED_MEMORY_BYTES.set(bytes);
    }

    fn inc_exhausted(&self, _reason: &str) {
        SCAN_ESTIMATED_MEMORY_REJECTED_TOTAL.inc();
    }
}

pub(crate) type ScanMemoryBudget = MemoryManager<ScanMemoryMetrics>;
pub(crate) type ScanMemoryGuard = MemoryGuard<ScanMemoryMetrics>;

pub(crate) fn new_scan_memory_budget(
    limit: MemoryLimit,
    total_memory: u64,
) -> Option<ScanMemoryBudget> {
    let bytes = limit.resolve(total_memory);
    if bytes == 0 {
        if !limit.is_unlimited() {
            common_telemetry::warn!(
                "Cannot resolve estimated scan memory budget; using file-count checks"
            );
        }
        return None;
    }
    Some(MemoryManager::with_granularity(
        bytes,
        PermitGranularity::Kilobyte,
        ScanMemoryMetrics,
    ))
}

/// Reserve one file's maximum row group for each independent reader. The manager
/// sums reservations across all active ranges, partitions, phases, and queries.
pub(crate) fn reserve(budget: &ScanMemoryBudget, bytes: u64) -> Result<ScanMemoryGuard> {
    // Check before permit conversion, which clamps very large requests.
    if bytes > budget.limit_bytes() {
        SCAN_ESTIMATED_MEMORY_REJECTED_TOTAL.inc();
        return ScanMemoryExhaustedSnafu {
            requested: bytes,
            reserved: budget.used_bytes(),
            limit: budget.limit_bytes(),
        }
        .fail();
    }
    budget.try_acquire(bytes).ok_or_else(|| {
        ScanMemoryExhaustedSnafu {
            requested: bytes,
            reserved: budget.used_bytes(),
            limit: budget.limit_bytes(),
        }
        .build()
    })
}

/// Keep the reservation with the source, including when it moves into a task.
/// Dropping an unpolled stream must also release its already-acquired guard.
pub(crate) fn hold_reservation<T: Send>(
    stream: impl Stream<Item = Result<T>> + Send,
    guard: Option<ScanMemoryGuard>,
) -> impl Stream<Item = Result<T>> + Send {
    async_stream::stream! {
        let mut stream = Box::pin(stream);
        let reservation = guard;
        while let Some(batch) = stream.next().await {
            match batch {
                Ok(batch) => yield Ok(batch),
                Err(error) => {
                    // Release before yielding the error: consumers need not poll again.
                    drop(stream);
                    drop(reservation);
                    yield Err(error);
                    return;
                }
            }
        }
    }
}
