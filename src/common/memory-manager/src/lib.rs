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
