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

use std::time::Instant;

use prometheus::{Histogram, IntCounter, IntGauge};

/// Caller-bound metrics for native cache loading. No collectors are registered here.
/// Requests count only `try_get_with` calls, including its cache hits and callers sharing
/// another request's load. Plain `get` calls are not counted. Loads count
/// only loader futures actually polled by the backend; their difference is not a waiter count.
#[derive(Clone)]
pub struct CacheMetrics {
    pub requests: OperationMetrics,
    pub loads: OperationMetrics,
}

/// Metrics for one operation, with labels bound once when constructing a cache.
/// Completion counters are mutually exclusive. Dropping an unfinished future is cancellation.
#[derive(Clone)]
pub struct OperationMetrics {
    pub started: IntCounter,
    pub succeeded: IntCounter,
    pub failed: IntCounter,
    pub cancelled: IntCounter,
    pub active: IntGauge,
    /// Wall-clock lifetime, including suspended time, for every outcome.
    pub elapsed: Histogram,
}

impl OperationMetrics {
    /// Starts tracking an operation and its elapsed time.
    pub(crate) fn start(&self) -> OperationGuard<'_> {
        self.started.inc();
        self.active.inc();
        OperationGuard {
            metrics: self,
            started: Instant::now(),
            completed: false,
        }
    }
}

/// Tracks an operation, recording cancellation if dropped before completion.
pub(crate) struct OperationGuard<'a> {
    metrics: &'a OperationMetrics,
    started: Instant,
    completed: bool,
}

impl OperationGuard<'_> {
    /// Records success or failure and completes the operation.
    pub(crate) fn finish<T, E>(mut self, result: &Result<T, E>) {
        if result.is_ok() {
            self.metrics.succeeded.inc();
        } else {
            self.metrics.failed.inc();
        }
        self.completed = true;
    }
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        if !self.completed {
            self.metrics.cancelled.inc();
        }
        self.metrics.active.dec();
        self.metrics
            .elapsed
            .observe(self.started.elapsed().as_secs_f64());
    }
}
