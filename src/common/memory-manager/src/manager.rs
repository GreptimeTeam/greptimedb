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

use std::sync::Arc;

use snafu::ensure;
use tokio::sync::{Semaphore, TryAcquireError};

use crate::error::{MemoryLimitExceededSnafu, MemorySemaphoreClosedSnafu, Result};
use crate::granularity::PermitGranularity;
use crate::guard::MemoryGuard;

/// Trait for recording memory usage metrics.
pub trait MemoryMetrics: Clone + Send + Sync + 'static {
    fn set_limit(&self, bytes: i64);
    fn set_in_use(&self, bytes: i64);
    fn inc_rejected(&self, reason: &str);
}

/// Generic memory manager for quota-controlled operations.
#[derive(Clone)]
pub struct MemoryManager<M: MemoryMetrics> {
    quota: Option<MemoryQuota<M>>,
}

#[derive(Clone)]
pub(crate) struct MemoryQuota<M: MemoryMetrics> {
    pub(crate) semaphore: Arc<Semaphore>,
    pub(crate) limit_permits: u32,
    pub(crate) granularity: PermitGranularity,
    pub(crate) metrics: M,
}

impl<M: MemoryMetrics> MemoryManager<M> {
    /// Creates a new memory manager with the given limit in bytes.
    /// `limit_bytes = 0` disables the limit.
    pub fn new(limit_bytes: u64, metrics: M) -> Self {
        Self::with_granularity(limit_bytes, PermitGranularity::default(), metrics)
    }

    /// Creates a new memory manager with specified granularity.
    pub fn with_granularity(limit_bytes: u64, granularity: PermitGranularity, metrics: M) -> Self {
        if limit_bytes == 0 {
            metrics.set_limit(0);
            return Self { quota: None };
        }

        let limit_permits = granularity.bytes_to_permits(limit_bytes);
        let limit_aligned_bytes = granularity.permits_to_bytes(limit_permits);
        metrics.set_limit(limit_aligned_bytes as i64);

        Self {
            quota: Some(MemoryQuota {
                semaphore: Arc::new(Semaphore::new(limit_permits as usize)),
                limit_permits,
                granularity,
                metrics,
            }),
        }
    }

    /// Returns the configured limit in bytes (0 if unlimited).
    pub fn limit_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| quota.permits_to_bytes(quota.limit_permits))
            .unwrap_or(0)
    }

    /// Returns currently used bytes.
    pub fn used_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| quota.permits_to_bytes(quota.used_permits()))
            .unwrap_or(0)
    }

    /// Returns available bytes.
    pub fn available_bytes(&self) -> u64 {
        self.quota
            .as_ref()
            .map(|quota| quota.permits_to_bytes(quota.available_permits_clamped()))
            .unwrap_or(0)
    }

    /// Acquires memory, waiting if necessary until enough is available.
    ///
    /// # Errors
    /// - Returns error if requested bytes exceed the total limit
    /// - Returns error if the semaphore is unexpectedly closed
    pub async fn acquire(&self, bytes: u64) -> Result<MemoryGuard<M>> {
        match &self.quota {
            None => Ok(MemoryGuard::unlimited()),
            Some(quota) => {
                let permits = quota.bytes_to_permits(bytes);

                ensure!(
                    permits <= quota.limit_permits,
                    MemoryLimitExceededSnafu {
                        requested_bytes: bytes,
                        limit_bytes: quota.permits_to_bytes(quota.limit_permits),
                    }
                );

                let permit = quota
                    .semaphore
                    .clone()
                    .acquire_many_owned(permits)
                    .await
                    .map_err(|_| MemorySemaphoreClosedSnafu.build())?;
                quota.update_in_use_metric();
                Ok(MemoryGuard::limited(permit, quota.clone()))
            }
        }
    }

    /// Tries to acquire memory. Returns Some(guard) on success, None if insufficient.
    pub fn try_acquire(&self, bytes: u64) -> Option<MemoryGuard<M>> {
        match &self.quota {
            None => Some(MemoryGuard::unlimited()),
            Some(quota) => {
                let permits = quota.bytes_to_permits(bytes);

                match quota.semaphore.clone().try_acquire_many_owned(permits) {
                    Ok(permit) => {
                        quota.update_in_use_metric();
                        Some(MemoryGuard::limited(permit, quota.clone()))
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        quota.metrics.inc_rejected("try_acquire");
                        None
                    }
                }
            }
        }
    }
}

impl<M: MemoryMetrics> MemoryQuota<M> {
    pub(crate) fn bytes_to_permits(&self, bytes: u64) -> u32 {
        self.granularity.bytes_to_permits(bytes)
    }

    pub(crate) fn permits_to_bytes(&self, permits: u32) -> u64 {
        self.granularity.permits_to_bytes(permits)
    }

    pub(crate) fn used_permits(&self) -> u32 {
        self.limit_permits
            .saturating_sub(self.available_permits_clamped())
    }

    pub(crate) fn available_permits_clamped(&self) -> u32 {
        self.semaphore
            .available_permits()
            .min(self.limit_permits as usize) as u32
    }

    pub(crate) fn update_in_use_metric(&self) {
        let bytes = self.permits_to_bytes(self.used_permits());
        self.metrics.set_in_use(bytes as i64);
    }
}
