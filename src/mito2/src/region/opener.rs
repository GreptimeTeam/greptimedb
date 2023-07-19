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

use crate::config::MitoConfig;
use crate::error::Result;
use crate::manifest::manager::RegionManifestManager;
use crate::manifest::options::RegionManifestOptions;
use crate::memtable::MemtableBuilderRef;
use crate::metadata::RegionMetadata;
use crate::region::version::{VersionBuilder, VersionControl};
use crate::region::MitoRegion;

/// Builder to create a new [MitoRegion] or open an existing one.
pub(crate) struct RegionOpener {
    metadata: RegionMetadata,
    memtable_builder: MemtableBuilderRef,
    object_store: ObjectStore,
    region_dir: String,
}

impl RegionOpener {
    /// Returns a new opener.
    pub(crate) fn new(
        metadata: RegionMetadata,
        memtable_builder: MemtableBuilderRef,
        object_store: ObjectStore,
    ) -> RegionOpener {
        RegionOpener {
            metadata,
            memtable_builder,
            object_store,
            region_dir: String::new(),
        }
    }

    /// Sets the region dir.
    pub(crate) fn region_dir(mut self, value: &str) -> Self {
        self.region_dir = value.to_string();
        self
    }

    /// Writes region manifest and creates a new region.
    pub(crate) async fn create(self, config: &MitoConfig) -> Result<MitoRegion> {
        let region_id = self.metadata.region_id;
        // Create a manifest manager for this region.
        let options = RegionManifestOptions {
            manifest_dir: new_manifest_dir(&self.region_dir),
            object_store: self.object_store,
            compress_type: config.manifest_compress_type,
            checkpoint_interval: config.manifest_checkpoint_interval,
            // We are creating a new region, so we need to set this field.
            initial_metadata: Some(self.metadata.clone()),
        };
        // Writes regions to the manifest file.
        let manifest_manager = RegionManifestManager::new(options).await?;

        let metadata = Arc::new(self.metadata);
        let mutable = self.memtable_builder.build(&metadata);

        let version = VersionBuilder::new(metadata, mutable).build();
        let version_control = Arc::new(VersionControl::new(version));

        Ok(MitoRegion {
            region_id,
            version_control,
            manifest_manager,
        })
    }
}

/// Returns the directory to the manifest files.
fn new_manifest_dir(region_dir: &str) -> String {
    join_dir(region_dir, "manifest")
}
