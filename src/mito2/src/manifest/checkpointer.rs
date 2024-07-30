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

use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use common_telemetry::{error, info};
use store_api::manifest::{ManifestVersion, MIN_VERSION};
use store_api::storage::RegionId;

use crate::manifest::action::{RegionCheckpoint, RegionManifest};
use crate::manifest::manager::RegionManifestOptions;
use crate::manifest::storage::ManifestObjectStore;
use crate::metrics::MANIFEST_OP_ELAPSED;

/// [`Checkpointer`] is responsible for doing checkpoint for a region, in an asynchronous way.
#[derive(Debug)]
pub(crate) struct Checkpointer {
    manifest_options: RegionManifestOptions,
    inner: Arc<Inner>,
}

#[derive(Debug)]
struct Inner {
    region_id: RegionId,
    manifest_store: ManifestObjectStore,
    last_checkpoint_version: AtomicU64,
    is_doing_checkpoint: AtomicBool,
}

impl Inner {
    async fn do_checkpoint(&self, checkpoint: RegionCheckpoint) {
        let _guard = scopeguard::guard(&self.is_doing_checkpoint, |x| {
            x.store(false, Ordering::Relaxed);
        });

        let _t = MANIFEST_OP_ELAPSED
            .with_label_values(&["checkpoint"])
            .start_timer();

        let region_id = self.region_id();
        let version = checkpoint.last_version();
        let checkpoint = match checkpoint.encode() {
            Ok(checkpoint) => checkpoint,
            Err(e) => {
                error!(e; "Failed to encode checkpoint {:?}", checkpoint);
                return;
            }
        };
        if let Err(e) = self
            .manifest_store
            .save_checkpoint(version, &checkpoint)
            .await
        {
            error!(e; "Failed to save checkpoint for region {}", region_id);
            return;
        }

        if let Err(e) = self.manifest_store.delete_until(version, true).await {
            error!(e; "Failed to delete manifest actions until version {} for region {}", version, region_id);
            return;
        }

        self.last_checkpoint_version
            .store(version, Ordering::Relaxed);

        info!(
            "Checkpoint for region {} success, version: {}",
            region_id, version
        );
    }

    fn region_id(&self) -> RegionId {
        self.region_id
    }

    fn is_doing_checkpoint(&self) -> bool {
        self.is_doing_checkpoint.load(Ordering::Relaxed)
    }

    fn set_doing_checkpoint(&self) {
        self.is_doing_checkpoint.store(true, Ordering::Relaxed);
    }
}

impl Checkpointer {
    pub(crate) fn new(
        region_id: RegionId,
        manifest_options: RegionManifestOptions,
        manifest_store: ManifestObjectStore,
        last_checkpoint_version: ManifestVersion,
    ) -> Self {
        Self {
            manifest_options,
            inner: Arc::new(Inner {
                region_id,
                manifest_store,
                last_checkpoint_version: AtomicU64::new(last_checkpoint_version),
                is_doing_checkpoint: AtomicBool::new(false),
            }),
        }
    }

    pub(crate) fn last_checkpoint_version(&self) -> ManifestVersion {
        self.inner.last_checkpoint_version.load(Ordering::Relaxed)
    }

    /// Check if it's needed to do checkpoint for the region by the checkpoint distance.
    /// If needed, and there's no currently running checkpoint task, it will start a new checkpoint
    /// task running in the background.
    pub(crate) fn maybe_do_checkpoint(&self, manifest: &RegionManifest) {
        if self.manifest_options.checkpoint_distance == 0 {
            return;
        }

        let last_checkpoint_version = self.last_checkpoint_version();
        if manifest.manifest_version - last_checkpoint_version
            < self.manifest_options.checkpoint_distance
        {
            return;
        }

        // We can simply check whether there's a running checkpoint task like this, all because of
        // the caller of this function is ran single threaded, inside the lock of RegionManifestManager.
        if self.inner.is_doing_checkpoint() {
            return;
        }

        let start_version = if last_checkpoint_version == 0 {
            // Checkpoint version can't be zero by implementation.
            // So last checkpoint version is zero means no last checkpoint.
            MIN_VERSION
        } else {
            last_checkpoint_version + 1
        };
        let end_version = manifest.manifest_version;
        info!(
            "Start doing checkpoint for region {}, compacted version: [{}, {}]",
            self.inner.region_id(),
            start_version,
            end_version,
        );

        let checkpoint = RegionCheckpoint {
            last_version: end_version,
            compacted_actions: (end_version - start_version + 1) as usize,
            checkpoint: Some(manifest.clone()),
        };
        self.do_checkpoint(checkpoint);
    }

    fn do_checkpoint(&self, checkpoint: RegionCheckpoint) {
        self.inner.set_doing_checkpoint();

        let inner = self.inner.clone();
        common_runtime::spawn_global(async move {
            inner.do_checkpoint(checkpoint).await;
        });
    }

    #[cfg(test)]
    pub(crate) fn is_doing_checkpoint(&self) -> bool {
        self.inner.is_doing_checkpoint()
    }
}
