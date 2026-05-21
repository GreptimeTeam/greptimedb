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

//! Flush hook extension point for SST and manifest operations.

use std::sync::Arc;

use async_trait::async_trait;
use store_api::ManifestVersion;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::manifest::action::RegionEdit;
use crate::sst::file::FileMeta;
use crate::sst::parquet::SstInfo;

/// Information about a single SST file written during flush.
pub struct SstFileInfo<'a> {
    pub sst_info_ref: &'a SstInfo,
    pub file_meta: &'a FileMeta,
}

/// Extension hook for flush operations.
///
/// Implementations can be registered via the `Plugins` system:
/// ```ignore
/// use std::sync::Arc;
/// use common_base::Plugins;
/// use mito2::engine::flush_hook::{FlushHook, FlushHookRef};
///
/// plugins.insert(Arc::new(MyHook) as FlushHookRef);
/// ```
#[async_trait]
pub trait FlushHook: Send + Sync {
    /// Called after SST files are written during flush.
    ///
    /// - `files`: per-file metadata (SstInfo + FileMeta) for each SST written.
    /// - `region_metadata`: provides the schema for column type information.
    async fn on_sst_files_written(
        &self,
        region_id: RegionId,
        region_metadata: &RegionMetadataRef,
        files: &[SstFileInfo<'_>],
    ) {
        let _ = (region_id, region_metadata, files);
    }

    /// Called after the region manifest is successfully updated.
    async fn on_manifest_updated(
        &self,
        region_id: RegionId,
        edit: &RegionEdit,
        manifest_version: ManifestVersion,
    ) {
        let _ = (region_id, edit, manifest_version);
    }
}

pub type FlushHookRef = Arc<dyn FlushHook>;
