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
    file_sizes: Arc<RwLock<HashMap<FileKey, u64>>>,
    total_size: Arc<AtomicU64>,
}

impl SizeTracker {
    /// Returns a new [SizeTracker].
    pub fn new(total_size: Arc<AtomicU64>) -> Self {
        Self {
            file_sizes: Arc::new(RwLock::new(HashMap::new())),
            total_size,
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

    /// Records a delta file size.
    pub(crate) fn record_delta(&self, version: ManifestVersion, size: u64) {
        self.record(FileKey::Delta(version), size);
    }

    /// Records a checkpoint file size.
    pub(crate) fn record_checkpoint(&self, version: ManifestVersion, size: u64) {
        self.record(FileKey::Checkpoint(version), size);
    }

    /// Removes a file from tracking.
    pub(crate) fn remove(&self, key: &FileKey) {
        if let Some(size) = self.file_sizes.write().unwrap().remove(key) {
            self.total_size.fetch_sub(size, Ordering::Relaxed);
        }
    }

    /// Returns the total tracked size.
    pub(crate) fn total(&self) -> u64 {
        self.total_size.load(Ordering::Relaxed)
    }

    /// Resets all tracking.
    pub(crate) fn reset(&self) {
        self.file_sizes.write().unwrap().clear();
        self.total_size.store(0, Ordering::Relaxed);
    }

    fn record(&self, key: FileKey, size: u64) {
        // Remove the old size if present
        if let Some(old_size) = self.file_sizes.write().unwrap().insert(key, size) {
            self.total_size.fetch_sub(old_size, Ordering::Relaxed);
        }
        self.total_size.fetch_add(size, Ordering::Relaxed);
    }
}
