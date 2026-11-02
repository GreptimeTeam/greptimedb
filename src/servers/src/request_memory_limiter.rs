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

//! Unified memory limiter for all server request protocols.

use std::sync::Arc;

use common_memory_manager::{MemoryGuard, MemoryManager, OnExhaustedPolicy, PermitGranularity};
use snafu::{IntoError, ResultExt};

use crate::error::{MemoryLimitExceededSnafu, Result};
use crate::request_memory_metrics::RequestMemoryMetrics;

/// Unified memory limiter for all server request protocols.
///
/// Manages a global memory pool for HTTP requests, gRPC messages, and
/// Arrow Flight batches without distinguishing between them.
#[derive(Clone)]
pub struct ServerMemoryLimiter {
    manager: Arc<MemoryManager<RequestMemoryMetrics>>,
    policy: OnExhaustedPolicy,
}

impl Default for ServerMemoryLimiter {
    /// Creates a limiter with unlimited memory (0 bytes) and default policy.
    fn default() -> Self {
        Self::new(0, OnExhaustedPolicy::default())
    }
}

impl ServerMemoryLimiter {
    /// Creates a new unified memory limiter.
    ///
    /// # Arguments
    ///
    /// * `total_bytes` - Maximum total memory for all concurrent requests (0 = unlimited)
    /// * `policy` - Policy when memory quota is exhausted
    pub fn new(total_bytes: u64, policy: OnExhaustedPolicy) -> Self {
        let manager = Arc::new(MemoryManager::with_granularity(
            total_bytes,
            PermitGranularity::Kilobyte,
            RequestMemoryMetrics,
        ));

        Self { manager, policy }
    }

    /// Acquire memory for a request.
    pub async fn acquire(&self, bytes: u64) -> Result<MemoryGuard<RequestMemoryMetrics>> {
        self.manager
            .acquire_with_policy(bytes, self.policy)
            .await
            .context(MemoryLimitExceededSnafu)
    }

    /// Try to acquire memory without waiting.
    pub fn try_acquire(&self, bytes: u64) -> Option<MemoryGuard<RequestMemoryMetrics>> {
        self.manager.try_acquire(bytes)
    }

    /// Rejects requests whose additional bytes cannot fit alongside their own held permits.
    pub(crate) fn check_request_fits(&self, bytes: u64, held_bytes: u64) -> Result<()> {
        let requested_bytes = held_bytes.checked_add(bytes);
        let limit_bytes = self.limit_bytes();
        match requested_bytes {
            Some(bytes) if limit_bytes == 0 || bytes <= limit_bytes => Ok(()),
            _ => Err(MemoryLimitExceededSnafu.into_error(
                common_memory_manager::Error::MemoryLimitExceeded {
                    requested_bytes: requested_bytes.unwrap_or(u64::MAX),
                    limit_bytes,
                },
            )),
        }
    }

    pub(crate) fn policy(&self) -> OnExhaustedPolicy {
        self.policy
    }

    /// Returns total memory limit in bytes (0 if unlimited).
    pub fn limit_bytes(&self) -> u64 {
        self.manager.limit_bytes()
    }

    /// Returns currently reserved (in-flight) bytes.
    pub fn used_bytes(&self) -> u64 {
        self.manager.used_bytes()
    }
}
