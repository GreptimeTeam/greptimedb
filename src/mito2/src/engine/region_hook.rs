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

//! Region hook extension point for observing SST writes and manifest mutations.
//!
//! ## Design
//!
//! The [`RegionHook`] trait provides two methods with clear separation of concerns:
//!
//! - [`on_sst_files_written`]: Fires when mito2 physically writes SST **data files**.
//!   Provides per-file [`SstInfo`] + [`FileMeta`] — the richest available metadata
//!   (row counts, index metadata, Parquet metadata, etc.).
//!
//! - [`on_manifest_updated`]: Fires after **any** manifest write is successfully committed.
//!   Receives the full [`RegionMetaActionList`] so consumers can inspect what changed
//!   (file additions/removals, schema changes, truncation, partition expression changes, etc.).
//!
//! Hook implementations are registered via the [`Plugins`](common_base::Plugins) system:
//! ```ignore
//! plugins.insert(Arc::new(MyHook) as RegionHookRef);
//! ```
//!
//! ## Coverage
//!
//! | Scenario                     | `on_sst_files_written` | `on_manifest_updated` |
//! |------------------------------|:----------------------:|:---------------------:|
//! | Flush (memtable → SST)       | ✅ Yes                 | ✅ Yes                |
//! | Local compaction             | ✅ Yes                 | ✅ Yes                |
//! | Remote compaction            | ✅ (compactor node) ¹     | ✅ (compactor node) ¹    |
//! | RegionEdit / bulk ingestion  | ❌ (files pre-written)  | ✅ Yes                |
//! | Copy region                  | ❌ (object-store copy)  | ✅ Yes                |
//! | Apply staging                | ❌ (delegates to edit)  | ✅ Yes ²               |
//! | Alter (schema change)        | ❌ (no SST files)       | ✅ Yes                |
//! | Truncate                     | ❌ (removes files)      | ✅ Yes                |
//! | Enter staging                | ❌ (no SST files)       | ✅ Yes                |
//! | Async index build            | ❌ (index files only)   | ✅ Yes                |
//!
//! ¹ Remote compaction runs on a dedicated compactor node via `open_compaction_region()`.
//!   The caller must pass plugins via `OpenCompactionRegionRequest` to enable hooks on the
//!   compactor node.
//! ² Apply staging fires `on_manifest_updated` twice: once when the staging SST files are
//!   committed via `RegionEdit`, and once when `exit_staging_on_success` merges all staged
//!   manifest actions into the live manifest.
//!
//! The following paths do **not** trigger any hook:
//! - Follower region sync / catchup (manifest read-only; followers don't author changes)
//! - GC / checkpoint / drop / remap (internal bookkeeping, not logical state changes)
//!
//! ## Invocation points
//!
//! `on_sst_files_written` is invoked at the SST write site (flush task or compaction task),
//! immediately after SST files are written but **before** the manifest is committed.
//!
//! `on_manifest_updated` is funneled through [`ManifestContext::update_locked`],
//! the sole caller of the low-level [`RegionManifestManager::update`], which
//! packages each successful write into a [`PendingManifestHook`]. The caller
//! owns the write lock, drops it, and *then* fires the receipt — the hook must
//! never run under the lock. [`ManifestContext::update_manifest`] is the common
//! case: it acquires the lock, delegates to `update_locked`, and fires the
//! receipt in one go. Multi-step sequences (staging-exit, role-state backfill)
//! call `update_locked` directly under their own held guard.
//!
//! Non-logical writes (GC, staging bookkeeping) call the manager's own methods
//! directly and intentionally do not fire the hook.
//!
//! ## Future work
//!
//! A future `on_files_removed` hook may be added to observe file lifecycle end
//! (GC, drop, truncate, compaction removal). This is not yet implemented.
//!
//! [`on_sst_files_written`]: RegionHook::on_sst_files_written
//! [`on_manifest_updated`]: RegionHook::on_manifest_updated
//! [`RegionManifestManager::update`]: crate::manifest::manager::RegionManifestManager::update
//! [`ManifestContext::update_locked`]: crate::region::ManifestContext::update_locked
//! [`ManifestContext::update_manifest`]: crate::region::ManifestContext::update_manifest

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::manifest::action::RegionMetaActionList;
use crate::sst::file::FileMeta;
use crate::sst::parquet::SstInfo;

/// A deferred [`RegionHook::on_manifest_updated`] notification produced by a
/// logical manifest write via [`ManifestContext::update_locked`](crate::region::ManifestContext::update_locked).
///
/// Must be [`fire`](Self::fire)d **after** the manifest write lock is released
/// (the hook may read the manifest). `#[must_use]` so a forgotten receipt warns.
#[must_use = "the region hook must be fired after releasing the manifest write lock"]
pub(crate) struct PendingManifestHook {
    region_id: RegionId,
    /// `None` when no hook is registered (fire becomes a no-op).
    action_list: Option<RegionMetaActionList>,
    version: ManifestVersion,
    hook: Option<RegionHookRef>,
}

impl PendingManifestHook {
    pub(crate) fn new(
        region_id: RegionId,
        action_list: Option<RegionMetaActionList>,
        version: ManifestVersion,
        hook: Option<RegionHookRef>,
    ) -> Self {
        Self {
            region_id,
            action_list,
            version,
            hook,
        }
    }

    /// The manifest version produced by the write.
    pub(crate) fn version(&self) -> ManifestVersion {
        self.version
    }

    /// Fires the hook if one is registered. Safe to call unconditionally: it is
    /// a no-op when no hook is registered.
    pub(crate) async fn fire(self) {
        if let (Some(hook), Some(action_list)) = (self.hook, self.action_list) {
            hook.on_manifest_updated(self.region_id, &action_list, self.version)
                .await;
        }
    }

    /// Merges two pending notifications into one so consumers observe a single
    /// `on_manifest_updated` call covering all actions. The combined action list
    /// keeps `self`'s actions followed by `other`'s, and the *later* manifest
    /// version wins. Used when a sequence of writes (e.g. staging-exit followed
    /// by metadata backfill) should notify the hook exactly once.
    pub(crate) fn merge(self, other: PendingManifestHook) -> PendingManifestHook {
        debug_assert_eq!(
            self.region_id, other.region_id,
            "Cannot merge pending hooks of different regions: {:?} and {:?}",
            self.region_id, other.region_id
        );
        PendingManifestHook {
            region_id: self.region_id,
            action_list: match (self.action_list, other.action_list) {
                (Some(mut a), Some(b)) => {
                    a.actions.extend(b.actions);
                    Some(a)
                }
                (a, None) => a,
                (None, b) => b,
            },
            version: self.version.max(other.version),
            hook: self.hook.or(other.hook),
        }
    }
}

/// Information about a single SST data file written during flush or compaction.
pub struct SstFileInfo<'a> {
    pub sst_info_ref: &'a SstInfo,
    pub file_meta: &'a FileMeta,
}

/// Hook for observing region mutations in mito2.
///
/// Implementations can be registered via the `Plugins` system:
/// ```ignore
/// use std::sync::Arc;
/// use common_base::Plugins;
/// use mito2::engine::region_hook::{RegionHook, RegionHookRef};
///
/// plugins.insert(Arc::new(MyHook) as RegionHookRef);
/// ```
#[async_trait]
pub trait RegionHook: Send + Sync + Debug {
    /// Called after SST **data files** are physically written, before manifest commit.
    ///
    /// This fires only when mito2 itself writes SST files (flush and compaction).
    /// It does **not** fire when SST files are pre-written externally (bulk ingestion,
    /// copy region) or when only index files are written (async index build).
    async fn on_sst_files_written(
        &self,
        region_id: RegionId,
        region_metadata: &RegionMetadataRef,
        files: &[SstFileInfo<'_>],
    ) {
        let _ = (region_id, region_metadata, files);
    }

    /// Called after the region manifest is successfully committed.
    ///
    /// Fires for **all** manifest write paths: flush, compaction, region edit,
    /// copy region, alter, truncate, enter staging, index build, etc.
    ///
    /// Does **not** fire for:
    /// - Manifest reads / follower sync (no write)
    /// - GC / checkpoint (internal bookkeeping)
    /// - Failed manifest updates
    async fn on_manifest_updated(
        &self,
        region_id: RegionId,
        action_list: &RegionMetaActionList,
        manifest_version: ManifestVersion,
    ) {
        let _ = (region_id, action_list, manifest_version);
    }
}

pub type RegionHookRef = Arc<dyn RegionHook>;
