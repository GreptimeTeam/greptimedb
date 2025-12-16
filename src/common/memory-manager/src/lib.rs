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

//! Generic memory management for resource-constrained operations.
//!
//! This crate provides a reusable memory quota system based on semaphores,
//! allowing different subsystems (compaction, flush, index build, etc.) to
//! share the same allocation logic while using their own metrics.

mod error;
mod guard;
mod manager;
mod policy;

#[cfg(test)]
mod tests;

pub use error::{Error, Result};
pub use guard::MemoryGuard;
pub use manager::{MemoryManager, MemoryMetrics, PERMIT_GRANULARITY_BYTES};
pub use policy::{DEFAULT_MEMORY_WAIT_TIMEOUT, OnExhaustedPolicy};

/// No-op metrics implementation for testing.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoOpMetrics;

impl MemoryMetrics for NoOpMetrics {
    #[inline(always)]
    fn set_limit(&self, _: i64) {}

    #[inline(always)]
    fn set_in_use(&self, _: i64) {}

    #[inline(always)]
    fn inc_rejected(&self, _: &str) {}
}
