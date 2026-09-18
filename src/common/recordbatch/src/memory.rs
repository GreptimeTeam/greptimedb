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

//! Shared query quota for reader reservations, batch streams, and DataFusion consumers.

use std::fmt;
use std::sync::Arc;

use common_memory_manager::{MemoryGuard, MemoryManager, MemoryMetrics, OnExhaustedPolicy};
use datafusion::execution::memory_pool::{
    GreedyMemoryPool, MemoryConsumer, MemoryLimit, MemoryPool, MemoryReservation,
    UnboundedMemoryPool,
};
use tokio::sync::Notify;

use crate::CallbackMemoryMetrics;

type AcquireResult = common_memory_manager::Result<()>;

/// Existing semaphore quota or a pool shared with DataFusion consumers.
#[derive(Clone)]
pub(crate) enum MemoryBackend {
    Manager(MemoryManager<CallbackMemoryMetrics>),
    Shared(Arc<QueryMemoryPool>),
}

impl MemoryBackend {
    pub(crate) fn shared(limit: usize, metrics: CallbackMemoryMetrics) -> Self {
        let inner: Arc<dyn MemoryPool> = if limit == 0 {
            Arc::new(UnboundedMemoryPool::default())
        } else {
            Arc::new(GreedyMemoryPool::new(limit))
        };
        Self::Shared(Arc::new(QueryMemoryPool {
            state: Arc::new(PoolState {
                inner,
                metrics,
                released: Notify::new(),
                limit,
            }),
            reject_on_failure: false,
        }))
    }

    pub(crate) fn current(&self) -> usize {
        match self {
            Self::Manager(manager) => manager.used_bytes() as usize,
            Self::Shared(pool) => pool.reserved(),
        }
    }

    pub(crate) fn limit(&self) -> usize {
        match self {
            Self::Manager(manager) => manager.limit_bytes() as usize,
            Self::Shared(pool) => pool.state.limit,
        }
    }

    pub(crate) fn memory_pool(&self) -> Option<Arc<dyn MemoryPool>> {
        match self {
            Self::Manager(_) => None,
            Self::Shared(pool) => Some(Arc::new(QueryMemoryPool {
                state: pool.state.clone(),
                reject_on_failure: true,
            })),
        }
    }

    pub(crate) fn reservation(&self) -> QueryMemoryReservation {
        let inner = match self {
            Self::Manager(manager) => Reservation::Manager(manager.try_acquire(0).unwrap()),
            Self::Shared(pool) => {
                let registration: Arc<dyn MemoryPool> = pool.clone();
                Reservation::Shared {
                    reservation: MemoryConsumer::new("query-reader").register(&registration),
                    pool: pool.clone(),
                }
            }
        };
        QueryMemoryReservation { inner }
    }
}

/// Reserved query memory, released automatically on drop.
#[derive(Debug)]
pub struct QueryMemoryReservation {
    inner: Reservation,
}

#[derive(Debug)]
enum Reservation {
    Manager(MemoryGuard<CallbackMemoryMetrics>),
    Shared {
        reservation: MemoryReservation,
        pool: Arc<QueryMemoryPool>,
    },
}

impl QueryMemoryReservation {
    pub(crate) fn try_acquire_additional(&mut self, bytes: u64) -> bool {
        match &mut self.inner {
            Reservation::Manager(guard) => guard.try_acquire_additional(bytes),
            Reservation::Shared { reservation, .. } => usize::try_from(bytes)
                .ok()
                .is_some_and(|bytes| reservation.try_grow(bytes).is_ok()),
        }
    }

    pub(crate) async fn acquire_additional_with_policy(
        &mut self,
        bytes: u64,
        policy: OnExhaustedPolicy,
    ) -> AcquireResult {
        let (reservation, pool) = match &mut self.inner {
            Reservation::Manager(guard) => {
                return guard.acquire_additional_with_policy(bytes, policy).await;
            }
            Reservation::Shared { reservation, pool } => (reservation, pool),
        };
        let limit = pool.state.limit;
        let exceeds = || common_memory_manager::Error::MemoryLimitExceeded {
            requested_bytes: bytes,
            limit_bytes: limit as u64,
        };
        let additional = usize::try_from(bytes).map_err(|_| exceeds())?;
        if reservation
            .size()
            .checked_add(additional)
            .is_none_or(|total| limit != 0 && total > limit)
        {
            return Err(exceeds());
        }
        match policy {
            OnExhaustedPolicy::Fail => reservation.try_grow(additional).map_err(|_| exceeds()),
            OnExhaustedPolicy::Wait { timeout } => {
                let acquire = async {
                    loop {
                        // Register before checking capacity so a concurrent release cannot be lost.
                        let released = pool.state.released.notified();
                        tokio::pin!(released);
                        released.as_mut().enable();
                        if reservation.try_grow(additional).is_ok() {
                            return;
                        }
                        released.await;
                    }
                };
                tokio::time::timeout(timeout, acquire).await.map_err(|_| {
                    common_memory_manager::Error::MemoryAcquireTimeout {
                        requested_bytes: bytes,
                        waited: timeout,
                    }
                })
            }
        }
    }
}

struct PoolState {
    inner: Arc<dyn MemoryPool>,
    metrics: CallbackMemoryMetrics,
    released: Notify,
    limit: usize,
}

/// Pool adapter that publishes usage and wakes asynchronous reservations on release.
pub(crate) struct QueryMemoryPool {
    state: Arc<PoolState>,
    // Async callers report rejection only after their wait policy is exhausted.
    reject_on_failure: bool,
}

impl fmt::Debug for QueryMemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMemoryPool")
            .field("inner", &self.state.inner)
            .finish()
    }
}

impl MemoryPool for QueryMemoryPool {
    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.state.inner.grow(reservation, additional);
        self.state.metrics.set_in_use(self.reserved() as i64);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.state.inner.shrink(reservation, shrink);
        self.state.metrics.set_in_use(self.reserved() as i64);
        self.state.released.notify_waiters();
    }

    fn try_grow(
        &self,
        reservation: &MemoryReservation,
        additional: usize,
    ) -> datafusion_common::Result<()> {
        // Guard the upstream pool's unchecked addition as well as the configured limit.
        let result = if self.reserved().checked_add(additional).is_none() {
            Err(datafusion_common::DataFusionError::ResourcesExhausted(
                "Query memory reservation overflow".into(),
            ))
        } else {
            self.state.inner.try_grow(reservation, additional)
        };
        match &result {
            Ok(()) => self.state.metrics.set_in_use(self.reserved() as i64),
            Err(_) => {
                self.state.metrics.inc_exhausted("try_grow");
                if self.reject_on_failure {
                    self.state.metrics.inc_rejected();
                }
            }
        }
        result
    }

    fn reserved(&self) -> usize {
        self.state.inner.reserved()
    }

    fn memory_limit(&self) -> MemoryLimit {
        self.state.inner.memory_limit()
    }
}
