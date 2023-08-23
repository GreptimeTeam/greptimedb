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

//! Region opener.

use std::sync::Arc;

use object_store::util::join_dir;
use object_store::ObjectStore;
use snafu::{ensure, OptionExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::RegionId;

use crate::config::MitoConfig;
use crate::error::{RegionCorruptedSnafu, RegionNotFoundSnafu, Result};
use crate::manifest::manager::{RegionManifestManager, RegionManifestOptions};
use crate::memtable::MemtableBuilderRef;
use crate::region::version::{VersionBuilder, VersionControl};
use crate::region::MitoRegion;

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener {
    region_id: RegionId,
    metadata: Option<RegionMetadata>,
    memtable_builder: MemtableBuilderRef,
    object_store: ObjectStore,
    region_dir: String,
}

impl RegionOpener {
    /// Returns a new opener.
    pub(crate) fn new(
        region_id: RegionId,
        memtable_builder: MemtableBuilderRef,
        object_store: ObjectStore,
    ) -> RegionOpener {
        RegionOpener {
            region_id,
            metadata: None,
            memtable_builder,
            object_store,
            region_dir: String::new(),
        }
    }

    /// Sets metadata of the region to create.
    pub(crate) fn metadata(mut self, metadata: RegionMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Sets the region dir.
    pub(crate) fn region_dir(mut self, value: &str) -> Self {
        self.region_dir = value.to_string();
        self
    }

    /// Writes region manifest and creates a new region.
    ///
    /// # Panics
    /// Panics if metadata is not set.
    pub(crate) async fn create(self, config: &MitoConfig) -> Result<MitoRegion> {
        let region_id = self.region_id;
        let metadata = Arc::new(self.metadata.unwrap());

        // Create a manifest manager for this region.
        let options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store: self.object_store,
            compress_type: config.manifest_compress_type,
            checkpoint_distance: config.manifest_checkpoint_distance,
        };
        // Writes regions to the manifest file.
        let manifest_manager = RegionManifestManager::new(metadata.clone(), options).await?;

        let mutable = self.memtable_builder.build(&metadata);

        let version = VersionBuilder::new(metadata, mutable).build();
        let version_control = Arc::new(VersionControl::new(version));

        Ok(MitoRegion {
            region_id,
            version_control,
            region_dir: self.region_dir,
            manifest_manager,
        })
    }

    /// Opens an existing region.
    ///
    /// Returns error if the region doesn't exist.
    pub(crate) async fn open(self, config: &MitoConfig) -> Result<MitoRegion> {
        let options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store: self.object_store,
            compress_type: config.manifest_compress_type,
            checkpoint_distance: config.manifest_checkpoint_distance,
        };
        let manifest_manager =
            RegionManifestManager::open(options)
                .await?
                .context(RegionNotFoundSnafu {
                    region_id: self.region_id,
                })?;

        let manifest = manifest_manager.manifest().await;
        let metadata = manifest.metadata.clone();

        ensure!(
            metadata.region_id == self.region_id,
            RegionCorruptedSnafu {
                region_id: self.region_id,
                reason: format!("region id in metadata is {}", metadata.region_id),
            }
        );

        let mutable = self.memtable_builder.build(&metadata);
        let version = VersionBuilder::new(metadata, mutable).build();
        let version_control = Arc::new(VersionControl::new(version));

        // TODO(yingwen): Replay.

        Ok(MitoRegion {
            region_id: self.region_id,
            version_control,
            region_dir: self.region_dir,
            manifest_manager,
        })
    }
}

/// Returns the directory to the manifest files.
fn new_manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}
