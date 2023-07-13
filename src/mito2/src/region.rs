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

pub(crate) mod columns;
pub(crate) mod metadata;
mod version;

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use store_api::storage::RegionId;

use crate::region::version::VersionControlRef;

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

pub(crate) type RegionMapRef = Arc<RegionMap>;
