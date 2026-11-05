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

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, MutexGuard};

use common_base::readable_size::ReadableSize;
use lazy_static::lazy_static;
use prometheus::{IntGaugeVec, register_int_gauge_vec};
use store_api::metrics::REGION_LABEL;
use store_api::storage::RegionId;

const MANIFEST_SIZE_METRIC_THRESHOLD_ENV: &str =
    "GREPTIME_REGION_MANIFEST_SIZE_METRIC_THRESHOLD_BYTES";
const DEFAULT_MANIFEST_SIZE_METRIC_THRESHOLD: ReadableSize = ReadableSize::mb(512);

/// Process-wide threshold, read once on first use. Zero exposes all live regions.
static MANIFEST_SIZE_METRIC_THRESHOLD: LazyLock<u64> = LazyLock::new(|| {
    let value = std::env::var(MANIFEST_SIZE_METRIC_THRESHOLD_ENV).ok();
    parse_manifest_size_metric_threshold(value.as_deref())
});

fn parse_manifest_size_metric_threshold(value: Option<&str>) -> u64 {
    match value {
        None => DEFAULT_MANIFEST_SIZE_METRIC_THRESHOLD.as_bytes(),
        Some(value) => value.parse().unwrap_or_else(|error| {
            common_telemetry::warn!(
                "Invalid {MANIFEST_SIZE_METRIC_THRESHOLD_ENV}={value:?}: {error}; expected non-negative bytes; using {DEFAULT_MANIFEST_SIZE_METRIC_THRESHOLD}"
            );
            DEFAULT_MANIFEST_SIZE_METRIC_THRESHOLD.as_bytes()
        }),
    }
}

lazy_static! {
    /// Manifest size of live regions above the threshold, from the region statistics counter.
    /// Falling to or below the threshold removes the series; it may reappear later.
    /// An absent series does not imply a zero size.
    pub(crate) static ref REGION_MANIFEST_SIZE: IntGaugeVec = register_int_gauge_vec!(
        "greptime_mito_region_manifest_size_bytes",
        "mito region manifest size in bytes",
        &[REGION_LABEL]
    ).unwrap();
}

/// Publishes the manifest size of `region_id`.
fn set_region_manifest_size(region_id: RegionId, size: u64) {
    let region_id = region_id.as_u64().to_string();
    REGION_MANIFEST_SIZE
        .with_label_values(&[&region_id])
        .set(size as i64);
}

/// Removes the per-region series. Idempotent.
pub(crate) fn remove_region_manifest_size(region_id: RegionId) {
    let region_id = region_id.as_u64().to_string();
    let _ = REGION_MANIFEST_SIZE.remove_label_values(&[&region_id]);
}

/// Shared publishing state of one region's manifest size gauge.
///
/// Publishing can run on the worker or the background checkpoint task
/// (`common_runtime::spawn_global`). Activation, deactivation and publishing share one
/// mutex, so a late publish can neither resurrect a series removed by
/// close/drop nor overwrite a successor instance's value.
#[derive(Debug)]
struct MetricState {
    active: bool,
    published: bool,
    threshold: u64,
    /// Test-only pause point between the active check and the gauge write.
    #[cfg(test)]
    hook: Option<TestHook>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestHook(Arc<dyn Fn() + Send + Sync>);

#[cfg(test)]
impl std::fmt::Debug for TestHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TestHook")
    }
}

#[cfg(test)]
impl TestHook {
    pub(crate) fn new(hook: impl Fn() + Send + Sync + 'static) -> Self {
        Self(Arc::new(hook))
    }

    fn call(&self) {
        (self.0)();
    }
}

/// Publishing state of one region's manifest size gauge.
///
/// Clones share publication rights. Only the instance in the live map may publish;
/// a replacement revokes the old instance's rights before publishing its size.
#[derive(Debug, Clone)]
pub(crate) struct RegionManifestSizeMetric {
    region_id: RegionId,
    state: Arc<Mutex<MetricState>>,
}

impl RegionManifestSizeMetric {
    pub(crate) fn new(region_id: RegionId) -> Self {
        Self::with_threshold(region_id, *MANIFEST_SIZE_METRIC_THRESHOLD)
    }

    pub(crate) fn with_threshold(region_id: RegionId, threshold: u64) -> Self {
        Self {
            region_id,
            state: Arc::new(Mutex::new(MetricState {
                active: false,
                published: false,
                threshold,
                #[cfg(test)]
                hook: None,
            })),
        }
    }

    /// Overrides the threshold without changing the process environment.
    #[cfg(test)]
    pub(crate) fn set_test_threshold(&self, threshold: u64) {
        let mut state = self.lock();
        assert!(state.active, "test region must already be live");
        state.threshold = threshold;
    }

    /// Installs a pause hook used by the deterministic concurrency tests.
    #[cfg(test)]
    pub(crate) fn set_test_hook(&self, hook: Option<TestHook>) {
        self.lock().hook = hook;
    }

    fn lock(&self) -> MutexGuard<'_, MetricState> {
        // Metrics must keep working after an unrelated panic poisons the lock.
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Activates publication and applies the threshold to the current total.
    ///
    /// The total is read while holding the lock: a concurrent writer that
    /// skipped publishing because the gauge was not active yet cannot be lost.
    /// Does not call [`Self::publish`] to avoid taking the lock twice.
    pub(crate) fn activate(&self, total: &AtomicU64) {
        let mut state = self.lock();
        state.active = true;
        let total = total.load(Ordering::Relaxed);
        self.write(&mut state, total);
    }

    /// Stops publishing. Called when the instance leaves the live map or is replaced.
    ///
    /// Removes the series before a successor instance can publish.
    pub(crate) fn deactivate(&self) {
        let mut state = self.lock();
        state.active = false;
        if state.published {
            remove_region_manifest_size(self.region_id);
            state.published = false;
        }
    }

    /// Writes `total` when active.
    ///
    /// Callers must hold the size tracker's write lock so a concurrent publisher
    /// cannot publish an older total afterwards.
    pub(crate) fn publish(&self, total: u64) {
        let mut state = self.lock();
        if state.active {
            self.write(&mut state, total);
        }
    }

    /// Writes the gauge. The caller keeps the state lock alive across this call,
    /// so the pause hook (and the write) cannot interleave with activate/deactivate.
    fn write(&self, state: &mut MetricState, total: u64) {
        #[cfg(test)]
        if let Some(hook) = &state.hook {
            hook.call();
        }
        if state.threshold == 0 || total > state.threshold {
            set_region_manifest_size(self.region_id, total);
            state.published = true;
        } else if state.published {
            remove_region_manifest_size(self.region_id);
            state.published = false;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::*;

    /// Looks up a sample without creating a series. Region ids must be unique per test.
    ///
    /// Shared by the engine lifecycle, size tracker, and metrics tests.
    pub(crate) fn manifest_metric_sample(region_id: RegionId) -> Option<i64> {
        use prometheus::core::Collector;
        let label = region_id.as_u64().to_string();
        let samples: Vec<_> = REGION_MANIFEST_SIZE
            .collect()
            .into_iter()
            .flat_map(|family| family.get_metric().to_vec())
            .filter(|metric| {
                metric
                    .get_label()
                    .iter()
                    .any(|pair| pair.name() == REGION_LABEL && pair.value() == label)
            })
            .collect();
        assert!(samples.len() <= 1);
        samples
            .first()
            .map(|metric| metric.get_gauge().as_ref().unwrap().value() as i64)
    }

    #[test]
    fn manifest_metric_threshold_parsing() {
        for (input, expected) in [
            (None, 536_870_912),
            (Some("536870912"), 536_870_912),
            (Some("123"), 123),
            (Some("0"), 0),
            (Some(""), 536_870_912),
            (Some("-1"), 536_870_912),
            (Some("512MiB"), 536_870_912),
            (Some("18446744073709551616"), 536_870_912),
        ] {
            assert_eq!(parse_manifest_size_metric_threshold(input), expected);
        }
    }

    #[rstest::rstest]
    #[case::filtered(100, 40)]
    #[case::unfiltered(0, 41)]
    fn manifest_metric_threshold_transitions(#[case] threshold: u64, #[case] sequence: u32) {
        let id = RegionId::new(u32::MAX, sequence);
        let metric = RegionManifestSizeMetric::with_threshold(id, threshold);
        metric.publish(101);
        assert_eq!(manifest_metric_sample(id), None);
        metric.activate(&AtomicU64::new(99));
        assert_eq!(manifest_metric_sample(id), (threshold == 0).then_some(99));
        for total in [100, 101, 150, 100, 99, 0, 101] {
            metric.publish(total);
            let expected = (threshold == 0 || total > threshold).then_some(total as i64);
            assert_eq!(manifest_metric_sample(id), expected);
            assert_eq!(metric.lock().published, expected.is_some());
        }
        metric.deactivate();
        assert_eq!(manifest_metric_sample(id), None);
        metric.publish(200);
        assert_eq!(manifest_metric_sample(id), None);
    }

    #[test]
    fn manifest_metric_replacement_below_threshold() {
        let id = RegionId::new(u32::MAX, 42);
        let old = RegionManifestSizeMetric::with_threshold(id, 100);
        old.activate(&AtomicU64::new(200));
        assert_eq!(manifest_metric_sample(id), Some(200));
        let new = RegionManifestSizeMetric::with_threshold(id, 100);
        // An inactive opener must not remove the live instance's series.
        new.publish(50);
        new.deactivate();
        assert_eq!(manifest_metric_sample(id), Some(200));
        old.deactivate();
        new.activate(&AtomicU64::new(50));
        assert_eq!(manifest_metric_sample(id), None);
        new.publish(150);
        old.publish(300);
        old.deactivate();
        assert_eq!(manifest_metric_sample(id), Some(150));
        new.deactivate();
    }

    #[rstest::rstest]
    #[case::remove(false, false)]
    #[case::replace(true, false)]
    #[case::threshold_removal(true, true)]
    fn publishing_holds_lock_through_gauge_write(
        #[case] replace: bool,
        #[case] below_threshold: bool,
    ) {
        let id = RegionId::new(
            u32::MAX,
            if below_threshold {
                43
            } else if replace {
                21
            } else {
                20
            },
        );
        remove_region_manifest_size(id);
        let old = RegionManifestSizeMetric::with_threshold(id, 5);
        old.activate(&AtomicU64::new(10));
        let new = RegionManifestSizeMetric::with_threshold(id, 0);
        let (paused_tx, paused_rx) = mpsc::channel();
        let (go_tx, go_rx) = mpsc::channel();
        let go_rx = Mutex::new(go_rx);
        old.set_test_hook(Some(TestHook::new(move || {
            paused_tx.send(()).unwrap();
            go_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
        })));
        let publisher = std::thread::spawn({
            let old = old.clone();
            move || old.publish(if below_threshold { 5 } else { 11 })
        });
        paused_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        // Directly prove the lock is held at the gauge write boundary. This fails
        // deterministically if publication releases its guard before writing.
        let held = matches!(
            old.state.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        );
        let (started_tx, started_rx) = mpsc::channel();
        let successor = std::thread::spawn({
            let old = old.clone();
            let new = new.clone();
            move || {
                started_tx.send(()).unwrap();
                old.deactivate();
                if replace {
                    new.activate(&AtomicU64::new(20));
                }
            }
        });
        let started = started_rx.recv_timeout(Duration::from_secs(5));
        // Release before assertions so a failing assertion cannot strand threads.
        go_tx.send(()).unwrap();
        publisher.join().unwrap();
        successor.join().unwrap();
        assert!(
            held,
            "publication must hold the descriptor lock through the write"
        );
        started.unwrap();
        old.set_test_hook(None);
        old.publish(1);
        assert_eq!(manifest_metric_sample(id), replace.then_some(20));
        new.deactivate();
        remove_region_manifest_size(id);
    }
}
