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
use tokio::sync::{OwnedSemaphorePermit, TryAcquireError};

use crate::manager::{MemoryMetrics, MemoryQuota};

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

    /// Tries to allocate additional memory during task execution.
    ///
    /// On success, merges the new memory into this guard and returns true.
    /// On failure, returns false and leaves this guard unchanged.
    pub fn request_additional(&mut self, bytes: u64) -> bool {
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
                        debug!("Allocated additional {} bytes", bytes);
                        true
                    }
                    Err(TryAcquireError::NoPermits) | Err(TryAcquireError::Closed) => {
                        quota.metrics.inc_rejected("request_additional");
                        false
                    }
                }
            }
        }
    }

    /// Releases a portion of granted memory back to the pool early,
    /// before the guard is dropped.
    ///
    /// Returns true if the release succeeds or is a no-op; false if the request exceeds granted.
    pub fn early_release_partial(&mut self, bytes: u64) -> bool {
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
                        debug!("Early released {} bytes from memory guard", released_bytes);
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
