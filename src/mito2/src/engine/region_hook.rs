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
//! | Remote compaction            | ✅ (on compactor node)  | ✅ (on compactor node) |
//! | RegionEdit / bulk ingestion  | ❌ (files pre-written)  | ✅ Yes                |
//! | Copy region                  | ❌ (object-store copy)  | ✅ Yes                |
//! | Apply staging                | ❌ (delegates to edit)  | ✅ Yes                |
//! | Alter (schema change)        | ❌ (no SST files)       | ✅ Yes                |
//! | Truncate                     | ❌ (removes files)      | ✅ Yes                |
//! | Enter staging                | ❌ (no SST files)       | ✅ Yes                |
//! | Async index build            | ❌ (index files only)   | ✅ Yes                |
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
//! `on_manifest_updated` is centralized in [`ManifestContext::update_manifest_with_state_check`],
//! so it automatically covers all manifest write paths that go through `ManifestContext`.
//!
//! ## Future work
//!
//! A future `on_files_removed` hook may be added to observe file lifecycle end
//! (GC, drop, truncate, compaction removal). This is not yet implemented.
//!
//! [`on_sst_files_written`]: RegionHook::on_sst_files_written
//! [`on_manifest_updated`]: RegionHook::on_manifest_updated

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::manifest::action::RegionMetaActionList;
use crate::sst::file::FileMeta;
use crate::sst::parquet::SstInfo;

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
