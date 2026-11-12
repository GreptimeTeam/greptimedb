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

use std::sync::{Arc, Mutex, Weak};
use std::time::Instant;

use prometheus::core::{Collector, Desc};
use prometheus::proto::{Gauge, MetricFamily};
use prometheus::{Histogram, IntCounter, IntGauge, IntGaugeVec, Opts};

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

/// Cache byte metrics combining manually maintained gauges and sampled cache weights.
/// Sampling does not force backend maintenance or include pending loader values.
#[derive(Clone)]
pub struct CacheSizeMetrics {
    gauges: IntGaugeVec,
    sources: Arc<Mutex<Vec<CacheSizeSource>>>,
}

/// A byte metric with labels bound for one or more cache instances.
#[derive(Clone)]
pub struct CacheSizeMetric {
    metrics: CacheSizeMetrics,
    gauge: IntGauge,
}

/// Keeps a cache instance registered for size sampling while any owner remains.
#[derive(Clone)]
pub struct CacheSizeRegistration {
    _reader: Arc<dyn Fn() -> u64 + Send + Sync>,
}

struct CacheSizeSource {
    gauge: IntGauge,
    read: Weak<dyn Fn() -> u64 + Send + Sync>,
}

impl CacheSizeMetrics {
    /// Creates an unregistered collector. Register a clone with the caller's registry.
    pub fn new(opts: Opts, labels: &[&str]) -> prometheus::Result<Self> {
        Ok(Self {
            gauges: IntGaugeVec::new(opts, labels)?,
            sources: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Returns the backing gauges for callers retaining manual accounting.
    pub fn gauges(&self) -> &IntGaugeVec {
        &self.gauges
    }

    /// Returns a manually maintained gauge for existing non-cache accounting.
    pub fn with_label_values(&self, labels: &[&str]) -> IntGauge {
        self.gauges.with_label_values(labels)
    }

    /// Binds labels for sampled cache weights. Instances sharing labels are summed.
    pub fn bind(&self, labels: &[&str]) -> CacheSizeMetric {
        CacheSizeMetric {
            metrics: self.clone(),
            gauge: self.gauges.with_label_values(labels),
        }
    }
}

impl CacheSizeMetric {
    /// Registers a cache-level weight reader without retaining it in the collector.
    /// Keep the returned registration alongside the cache, including in its clones.
    pub fn register(
        &self,
        read: impl Fn() -> u64 + Send + Sync + 'static,
    ) -> CacheSizeRegistration {
        let read: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(read);
        self.metrics
            .sources
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(CacheSizeSource {
                gauge: self.gauge.clone(),
                read: Arc::downgrade(&read),
            });
        CacheSizeRegistration { _reader: read }
    }
}

impl Collector for CacheSizeMetrics {
    fn desc(&self) -> Vec<&Desc> {
        self.gauges.desc()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        let mut families = self.gauges.collect();
        let mut sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
        sources.retain(|source| {
            let Some(read) = source.read.upgrade() else {
                return false;
            };
            let labels = source.gauge.collect();
            for family in &mut families {
                for metric in family.mut_metric() {
                    if metric.get_label() == labels[0].get_metric()[0].get_label() {
                        let bytes = metric
                            .get_gauge()
                            .as_ref()
                            .map_or(0.0, |gauge| gauge.value())
                            + read() as f64;
                        let mut gauge = Gauge::default();
                        gauge.set_value(bytes);
                        metric.set_gauge(gauge);
                    }
                }
            }
            true
        });
        families
    }
}
