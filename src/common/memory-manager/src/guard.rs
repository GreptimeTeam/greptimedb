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

use std::{fmt, mem};

use common_telemetry::debug;
use snafu::ensure;
use tokio::sync::{OwnedSemaphorePermit, TryAcquireError};

use crate::error::{
    MemoryAcquireTimeoutSnafu, MemoryLimitExceededSnafu, MemorySemaphoreClosedSnafu, Result,
};
use crate::manager::{MemoryMetrics, MemoryQuota};
use crate::policy::OnExhaustedPolicy;

/// Guard representing a slice of reserved memory.
pub struct MemoryGuard<M: MemoryMetrics> {
    pub(crate) state: GuardState<M>,
}

pub(crate) enum GuardState<M: MemoryMetrics> {
    Unlimited,
    Limited {
        permit: OwnedSemaphorePermit,
        quota: MemoryQuota<M>,
    },
}

impl<M: MemoryMetrics> MemoryGuard<M> {
    pub(crate) fn unlimited() -> Self {
        Self {
            state: GuardState::Unlimited,
        }
    }

    pub(crate) fn limited(permit: OwnedSemaphorePermit, quota: MemoryQuota<M>) -> Self {
        Self {
            state: GuardState::Limited { permit, quota },
        }
    }

    /// Returns granted quota in bytes.
    pub fn granted_bytes(&self) -> u64 {
        match &self.state {
            GuardState::Unlimited => 0,
            GuardState::Limited { permit, quota } => {
                quota.permits_to_bytes(permit.num_permits() as u32)
            }
        }
    }

    /// Acquires additional memory, waiting if necessary until enough is available.
    ///
    /// On success, merges the new memory into this guard.
    ///
    /// # Errors
    /// - Returns error if requested bytes would exceed the manager's total limit
    /// - Returns error if the semaphore is unexpectedly closed
    pub async fn acquire_additional(&mut self, bytes: u64) -> Result<()> {
        match &mut self.state {
            GuardState::Unlimited => Ok(()),
            GuardState::Limited { permit, quota } => {
                if bytes == 0 {
                    return Ok(());
                }

                let additional_permits = quota.bytes_to_permits(bytes);
                let current_permits = permit.num_permits() as u32;

                ensure!(
                    current_permits.saturating_add(additional_permits) <= quota.limit_permits,
                    MemoryLimitExceededSnafu {
                        requested_bytes: bytes,
                        limit_bytes: quota.permits_to_bytes(quota.limit_permits)
                    }
                );

                let additional_permit = quota
                    .semaphore
                    .clone()
                    .acquire_many_owned(additional_permits)
                    .await
                    .map_err(|_| MemorySemaphoreClosedSnafu.build())?;

                permit.merge(additional_permit);
                quota.update_in_use_metric();
                debug!("Acquired additional {} bytes", bytes);
                Ok(())
            }
        }
    }

    /// Tries to acquire additional memory without waiting.
    ///
    /// On success, merges the new memory into this guard and returns true.
    /// On failure, returns false and leaves this guard unchanged.
    pub fn try_acquire_additional(&mut self, bytes: u64) -> bool {
        match &mut self.state {
            GuardState::Unlimited => true,
            GuardState::Limited { permit, quota } => {
                if bytes == 0 {
                    return true;
                }

                let additional_permits = quota.bytes_to_permits(bytes);

                match quota
                    .semaphore
                    .clone()
                    .try_acquire_many_owned(additional_permits)
                {
                    Ok(additional_permit) => {
                        permit.merge(additional_permit);
                        quota.update_in_use_metric();
                        debug!("Acquired additional {} bytes", bytes);
                        true
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        quota.metrics.inc_rejected("try_acquire_additional");
                        false
                    }
                }
            }
        }
    }

    /// Acquires additional memory based on the given policy.
    ///
    /// - For `OnExhaustedPolicy::Wait`: Waits up to the timeout duration for memory to become available
    /// - For `OnExhaustedPolicy::Fail`: Returns immediately if memory is not available
    ///
    /// # Errors
    /// - `MemoryLimitExceeded`: Requested bytes would exceed the total limit (both policies), or memory is currently exhausted (Fail policy only)
    /// - `MemoryAcquireTimeout`: Timeout elapsed while waiting for memory (Wait policy only)
    /// - `MemorySemaphoreClosed`: The internal semaphore is unexpectedly closed (rare, indicates system issue)
    pub async fn acquire_additional_with_policy(
        &mut self,
        bytes: u64,
        policy: OnExhaustedPolicy,
    ) -> Result<()> {
        match policy {
            OnExhaustedPolicy::Wait { timeout } => {
                match tokio::time::timeout(timeout, self.acquire_additional(bytes)).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e),
                    Err(_elapsed) => MemoryAcquireTimeoutSnafu {
                        requested_bytes: bytes,
                        waited: timeout,
                    }
                    .fail(),
                }
            }
            OnExhaustedPolicy::Fail => {
                if self.try_acquire_additional(bytes) {
                    Ok(())
                } else {
                    MemoryLimitExceededSnafu {
                        requested_bytes: bytes,
                        limit_bytes: match &self.state {
                            GuardState::Unlimited => 0,
                            GuardState::Limited { quota, .. } => {
                                quota.permits_to_bytes(quota.limit_permits)
                            }
                        },
                    }
                    .fail()
                }
            }
        }
    }

    /// Releases a portion of granted memory back to the pool before the guard is dropped.
    ///
    /// Returns true if the release succeeds or is a no-op; false if the request exceeds granted.
    pub fn release_partial(&mut self, bytes: u64) -> bool {
        match &mut self.state {
            GuardState::Unlimited => true,
            GuardState::Limited { permit, quota } => {
                if bytes == 0 {
                    return true;
                }

                let release_permits = quota.bytes_to_permits(bytes);

                match permit.split(release_permits as usize) {
                    Some(released_permit) => {
                        let released_bytes =
                            quota.permits_to_bytes(released_permit.num_permits() as u32);
                        drop(released_permit);
                        quota.update_in_use_metric();
                        debug!("Released {} bytes from memory guard", released_bytes);
                        true
                    }
                    None => false,
                }
            }
        }
    }
}

impl<M: MemoryMetrics> Drop for MemoryGuard<M> {
    fn drop(&mut self) {
        if let GuardState::Limited { permit, quota } =
            mem::replace(&mut self.state, GuardState::Unlimited)
        {
            let bytes = quota.permits_to_bytes(permit.num_permits() as u32);
            drop(permit);
            quota.update_in_use_metric();
            debug!("Released memory: {} bytes", bytes);
        }
    }
}

impl<M: MemoryMetrics> fmt::Debug for MemoryGuard<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryGuard")
            .field("granted_bytes", &self.granted_bytes())
            .finish()
    }
}
