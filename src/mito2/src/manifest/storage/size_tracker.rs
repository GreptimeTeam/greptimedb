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

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use store_api::ManifestVersion;

use crate::metrics::RegionManifestSizeMetric;

/// Key to identify a manifest file.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub(crate) enum FileKey {
    /// A delta file (`.json`).
    Delta(ManifestVersion),
    /// A checkpoint file (`.checkpoint`).
    Checkpoint(ManifestVersion),
}

pub(crate) trait Tracker: Send + Sync + Debug {
    fn record(&self, version: ManifestVersion, size: u64);
}

#[derive(Debug, Clone)]
pub struct CheckpointTracker {
    size_tracker: SizeTracker,
}

impl Tracker for CheckpointTracker {
    fn record(&self, version: ManifestVersion, size: u64) {
        self.size_tracker.record(FileKey::Checkpoint(version), size);
    }
}

#[derive(Debug, Clone)]
pub struct DeltaTracker {
    size_tracker: SizeTracker,
}

impl Tracker for DeltaTracker {
    fn record(&self, version: ManifestVersion, size: u64) {
        self.size_tracker.record(FileKey::Delta(version), size);
    }
}

#[derive(Debug, Clone)]
pub struct NoopTracker;

impl Tracker for NoopTracker {
    fn record(&self, _version: ManifestVersion, _size: u64) {
        // noop
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SizeTracker {
    inventory: Arc<RwLock<Inventory>>,
    total_size: Arc<AtomicU64>,
    metric: Option<RegionManifestSizeMetric>,
}

#[derive(Debug, Default)]
struct Inventory {
    file_sizes: HashMap<FileKey, u64>,
    checkpoint_version: Option<ManifestVersion>,
}

impl SizeTracker {
    /// Returns a new [SizeTracker].
    pub fn new(total_size: Arc<AtomicU64>, metric: Option<RegionManifestSizeMetric>) -> Self {
        Self {
            inventory: Arc::default(),
            total_size,
            metric,
        }
    }

    /// Returns the manifest tracker.
    pub(crate) fn manifest_tracker(&self) -> DeltaTracker {
        DeltaTracker {
            size_tracker: self.clone(),
        }
    }

    /// Returns the checkpoint tracker.
    pub(crate) fn checkpoint_tracker(&self) -> CheckpointTracker {
        CheckpointTracker {
            size_tracker: self.clone(),
        }
    }

    /// Returns the total tracked size.
    pub(crate) fn total(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Whether this checkpoint's stored size is already known.
    pub(crate) fn contains_checkpoint(&self, version: ManifestVersion) -> bool {
        self.inventory
            .read()
            .unwrap()
            .file_sizes
            .contains_key(&FileKey::Checkpoint(version))
    }

    /// Keeps the latest observed checkpoint and deltas after it, regardless of cleanup.
    /// Physical storage may retain older checkpoints and deltas for longer.
    pub(crate) fn retain_after(&self, version: ManifestVersion) {
        let mut inventory = self.inventory.write().unwrap();
        if inventory
            .checkpoint_version
            .is_some_and(|current| current > version)
        {
            return;
        }
        inventory.checkpoint_version = Some(version);
        inventory.file_sizes.retain(|key, _| match key {
            FileKey::Delta(v) => *v > version,
            FileKey::Checkpoint(v) => *v >= version,
        });
        self.total_size
            .store(inventory.file_sizes.values().sum(), Ordering::Relaxed);
        self.publish_locked();
    }

    /// Called with the inventory write lock held, preserving publication order.
    fn publish_locked(&self) {
        if let Some(metric) = &self.metric {
            metric.publish(self.total_size.load(Ordering::Relaxed));
        }
    }

    fn record(&self, key: FileKey, size: u64) {
        let mut inventory = self.inventory.write().unwrap();
        // Manifest versions are monotonic: deltas at or below the checkpoint watermark
        // are superseded, even if a read started before publication finishes afterward.
        // Such a late read must not resurrect old entries.
        if inventory
            .checkpoint_version
            .is_some_and(|version| match key {
                FileKey::Delta(v) => v <= version,
                FileKey::Checkpoint(v) => v < version,
            })
        {
            return;
        }
        if let Some(old_size) = inventory.file_sizes.insert(key, size) {
            self.total_size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);
        self.publish_locked();
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::RegionId;

    use super::*;
    use crate::metrics::{manifest_metric_sample, remove_region_manifest_size};

    #[test]
    fn manifest_metric_tracks_mutations_and_activation() {
        let id = RegionId::new(u32::MAX, 30);
        remove_region_manifest_size(id);
        let total = Arc::new(AtomicU64::new(0));
        let metric = RegionManifestSizeMetric::with_threshold(id, 0);
        let tracker = SizeTracker::new(total.clone(), Some(metric.clone()));
        tracker.manifest_tracker().record(0, 10);
        assert_eq!(manifest_metric_sample(id), None);
        metric.activate(&total);
        assert_eq!(manifest_metric_sample(id), Some(10));
        tracker.manifest_tracker().record(0, 20);
        tracker.checkpoint_tracker().record(1, 30);
        assert_eq!(manifest_metric_sample(id), Some(tracker.total() as i64));
        assert_eq!(tracker.total(), 50);
        tracker.retain_after(1);
        assert_eq!(manifest_metric_sample(id), Some(30));
        // Late reads and stale retention cannot resurrect superseded bytes or gauges.
        tracker.manifest_tracker().record(0, 999);
        tracker.retain_after(0);
        assert_eq!(manifest_metric_sample(id), Some(30));
        // An unregistered opener with the same id has no right to publish.
        let candidate = RegionManifestSizeMetric::with_threshold(id, 0);
        let candidate_tracker = SizeTracker::new(Arc::new(AtomicU64::new(0)), Some(candidate));
        candidate_tracker.manifest_tracker().record(2, 999);
        assert_eq!(manifest_metric_sample(id), Some(30));
        tracker.retain_after(2);
        assert_eq!(manifest_metric_sample(id), Some(0));
        metric.deactivate();
        remove_region_manifest_size(id);
        tracker.manifest_tracker().record(3, 99);
        assert_eq!(manifest_metric_sample(id), None);
    }

    #[test]
    fn manifest_metric_retention_removes_series_below_threshold() {
        let id = RegionId::new(u32::MAX, 44);
        let total = Arc::new(AtomicU64::new(0));
        let metric = RegionManifestSizeMetric::with_threshold(id, 100);
        let tracker = SizeTracker::new(total.clone(), Some(metric.clone()));
        metric.activate(&total);
        tracker.manifest_tracker().record(0, 80);
        assert_eq!(tracker.total(), 80);
        assert_eq!(manifest_metric_sample(id), None);
        tracker.checkpoint_tracker().record(1, 30);
        assert_eq!(manifest_metric_sample(id), Some(110));
        tracker.retain_after(1);
        assert_eq!(tracker.total(), 30);
        assert_eq!(manifest_metric_sample(id), None);
        tracker.manifest_tracker().record(0, 999);
        assert_eq!(tracker.total(), 30);
        assert_eq!(manifest_metric_sample(id), None);
        metric.deactivate();
    }

    #[test]
    fn manifest_metric_publishes_with_inventory_locked() {
        use crate::metrics::TestHook;
        let id = RegionId::new(u32::MAX, 31);
        remove_region_manifest_size(id);
        let total = Arc::new(AtomicU64::new(0));
        let metric = RegionManifestSizeMetric::with_threshold(id, 0);
        let tracker = SizeTracker::new(total.clone(), Some(metric.clone()));
        metric.activate(&total);
        let sizes = tracker.inventory.clone();
        metric.set_test_hook(Some(TestHook::new(move || {
            assert!(
                matches!(sizes.try_write(), Err(std::sync::TryLockError::WouldBlock)),
                "inventory must remain locked through gauge publication"
            );
        })));
        tracker.manifest_tracker().record(0, 10);
        tracker.manifest_tracker().record(0, 20);
        tracker.checkpoint_tracker().record(1, 30);
        tracker.retain_after(1);
        tracker.retain_after(2);
        assert_eq!(manifest_metric_sample(id), Some(tracker.total() as i64));
        metric.deactivate();
        remove_region_manifest_size(id);
    }

    #[test]
    fn retain_checkpoint_and_later_deltas() {
        let tracker = SizeTracker::default();
        tracker.retain_after(0);
        assert_eq!(tracker.total(), 0);
        for (key, size) in [
            (FileKey::Delta(1), 10),
            (FileKey::Delta(2), 20),
            (FileKey::Delta(3), 30),
            (FileKey::Checkpoint(1), 40),
            (FileKey::Checkpoint(2), 50),
        ] {
            tracker.record(key, size);
        }
        tracker.retain_after(2);
        assert_eq!(tracker.total(), 80);
        assert_eq!(
            tracker.inventory.read().unwrap().file_sizes,
            HashMap::from([(FileKey::Delta(3), 30), (FileKey::Checkpoint(2), 50)])
        );
        // Late reads and an older retention request cannot roll accounting back.
        tracker.record(FileKey::Delta(2), 20);
        tracker.record(FileKey::Checkpoint(1), 40);
        tracker.retain_after(1);
        assert_eq!(tracker.total(), 80);
        tracker.record(FileKey::Delta(3), 35);
        assert_eq!(tracker.total(), 85);
    }
}
