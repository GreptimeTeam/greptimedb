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

//! Mito region.

mod version;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use store_api::storage::RegionId;

use crate::memtable::MemtableBuilderRef;
use crate::metadata::RegionMetadataRef;
use crate::region::version::{VersionBuilder, VersionControl, VersionControlRef};

/// Type to store region version.
pub type VersionNumber = u32;

/// Metadata and runtime status of a region.
#[derive(Debug)]
pub(crate) struct MitoRegion {
    version_control: VersionControlRef,
}

pub(crate) type MitoRegionRef = Arc<MitoRegion>;

/// Regions indexed by ids.
#[derive(Debug, Default)]
pub(crate) struct RegionMap {
    regions: RwLock<HashMap<RegionId, MitoRegionRef>>,
}

impl RegionMap {
    /// Returns true if the region exists.
    pub(crate) fn is_region_exists(&self, region_id: RegionId) -> bool {
        let regions = self.regions.read().unwrap();
        regions.contains_key(&region_id)
    }
}

pub(crate) type RegionMapRef = Arc<RegionMap>;

/// [MitoRegion] builder.
pub(crate) struct RegionBuilder {
    metadata: RegionMetadataRef,
    memtable_builder: MemtableBuilderRef,
}

impl RegionBuilder {
    /// Returns a new builder.
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        memtable_builder: MemtableBuilderRef,
    ) -> RegionBuilder {
        RegionBuilder {
            metadata,
            memtable_builder,
        }
    }

    /// Builds a new region.
    pub(crate) fn build(self) -> MitoRegion {
        let mutable = self.memtable_builder.build(&self.metadata);

        let version = VersionBuilder::new(self.metadata, mutable).build();
        let version_control = Arc::new(VersionControl::new(version));

        MitoRegion { version_control }
    }
}
