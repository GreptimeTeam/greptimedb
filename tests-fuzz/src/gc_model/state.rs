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

use std::collections::{BTreeMap, HashMap, HashSet};

pub type RegionId = u64;
pub type FileId = u128;
pub type Ts = i64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Leader,
    Follower,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lifecycle {
    Open,
    Dropped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FileRef {
    pub file_id: FileId,
    pub index_version: Option<u32>,
}

impl FileRef {
    pub fn new(file_id: FileId, index_version: Option<u32>) -> Self {
        Self {
            file_id,
            index_version,
        }
    }
}

#[derive(Clone, Debug)]
pub struct RegionState {
    pub role: Role,
    pub lifecycle: Lifecycle,
    pub manifest_version: u64,
    pub previous_manifest_version: u64,
    pub manifest_files: HashSet<FileRef>,
    pub previous_manifest_files: HashSet<FileRef>,
    pub removed_files: BTreeMap<Ts, HashSet<FileRef>>,
    pub previous_removed_files: BTreeMap<Ts, HashSet<FileRef>>,
}

impl RegionState {
    pub fn new(role: Role) -> Self {
        Self {
            role,
            lifecycle: Lifecycle::Open,
            manifest_version: 0,
            previous_manifest_version: 0,
            manifest_files: HashSet::new(),
            previous_manifest_files: HashSet::new(),
            removed_files: BTreeMap::new(),
            previous_removed_files: BTreeMap::new(),
        }
    }

    pub fn checkpoint_manifest_view(&mut self) {
        self.previous_manifest_version = self.manifest_version;
        self.previous_manifest_files = self.manifest_files.clone();
        self.previous_removed_files = self.removed_files.clone();
    }
}

#[derive(Clone, Debug, Default)]
pub struct TempRefsSnapshot {
    pub manifest_version_by_region: HashMap<RegionId, u64>,
    pub file_refs_by_region: HashMap<RegionId, HashSet<FileRef>>,
    pub cross_region_refs: HashMap<RegionId, HashSet<RegionId>>,
}

#[derive(Clone, Debug)]
pub struct ModelState {
    pub now: Ts,
    pub regions: HashMap<RegionId, RegionState>,
    pub temp_refs: TempRefsSnapshot,
    pub all_files: HashSet<FileRef>,
    pub lingering_millis: i64,
}

#[derive(Clone, Debug)]
pub struct ObservedRegionState {
    pub role: Role,
    pub lifecycle: Lifecycle,
    pub manifest_version: u64,
    pub manifest_files: HashSet<FileRef>,
    pub removed_files: BTreeMap<Ts, HashSet<FileRef>>,
}

#[derive(Clone, Debug, Default)]
pub struct ObservedSnapshot {
    pub now: Ts,
    pub regions: HashMap<RegionId, ObservedRegionState>,
    pub temp_refs: TempRefsSnapshot,
    pub mismatched_regions: HashSet<RegionId>,
}

impl ModelState {
    pub fn new(lingering_millis: i64) -> Self {
        Self {
            now: 0,
            regions: HashMap::new(),
            temp_refs: TempRefsSnapshot::default(),
            all_files: HashSet::new(),
            lingering_millis,
        }
    }
}

impl ObservedSnapshot {
    pub fn from_actual(state: &ModelState) -> Self {
        let regions = state
            .regions
            .iter()
            .map(|(region_id, region)| {
                (
                    *region_id,
                    ObservedRegionState {
                        role: region.role,
                        lifecycle: region.lifecycle,
                        manifest_version: region.manifest_version,
                        manifest_files: region.manifest_files.clone(),
                        removed_files: region.removed_files.clone(),
                    },
                )
            })
            .collect();

        Self {
            now: state.now,
            regions,
            temp_refs: state.temp_refs.clone(),
            mismatched_regions: HashSet::new(),
        }
    }
}
