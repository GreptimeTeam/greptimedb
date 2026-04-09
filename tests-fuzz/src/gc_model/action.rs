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

use rand::Rng;

use crate::gc_model::state::{FileRef, ModelState, RegionId, Role};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GcMode {
    Fast,
    Full,
}

#[derive(Clone, Debug)]
pub enum Action {
    CreateRegion {
        id: RegionId,
        role: Role,
    },
    DropRegion {
        id: RegionId,
    },
    PromoteFollower {
        id: RegionId,
    },
    DemoteLeader {
        id: RegionId,
    },
    CreateFileInManifest {
        region: RegionId,
        file: FileRef,
    },
    MoveFileToRemoved {
        region: RegionId,
        file: FileRef,
    },
    AddTempRef {
        region: RegionId,
        file: FileRef,
    },
    RemoveTempRef {
        region: RegionId,
        file: FileRef,
    },
    AddCrossRegionRef {
        src_region: RegionId,
        dst_region: RegionId,
    },
    RemoveCrossRegionRef {
        src_region: RegionId,
        dst_region: RegionId,
    },
    AdvanceTime {
        delta_ms: i64,
    },
    RunGc {
        mode: GcMode,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct ActionGenerator {
    pub max_regions: u8,
}

impl ActionGenerator {
    pub fn sample<R: Rng>(&self, state: &ModelState, rng: &mut R) -> Action {
        if self.has_liveness_pressure(state) && rng.random_bool(0.35) {
            return if rng.random_bool(0.45) {
                Action::AdvanceTime {
                    delta_ms: rng.random_range(1..=5000),
                }
            } else {
                Action::RunGc {
                    mode: if rng.random_bool(0.5) {
                        GcMode::Full
                    } else {
                        GcMode::Fast
                    },
                }
            };
        }

        if self.has_follower_cross_region_pressure(state) && rng.random_bool(0.25) {
            return if rng.random_bool(0.5) {
                Action::RunGc { mode: GcMode::Full }
            } else {
                self.sample_ref_action(state, rng)
            };
        }

        if self.has_manifest_activity(state) && rng.random_bool(0.15) {
            return Action::RunGc { mode: GcMode::Fast };
        }

        let roll = rng.random_range(0..100u8);
        if roll < 20 {
            return Action::AdvanceTime {
                delta_ms: rng.random_range(1..=5000),
            };
        }

        if roll < 40 {
            return self.sample_ref_action(state, rng);
        }

        if roll < 80 {
            return self.sample_file_or_topology_action(state, rng);
        }

        Action::RunGc {
            mode: if rng.random_bool(0.7) {
                GcMode::Fast
            } else {
                GcMode::Full
            },
        }
    }

    fn sample_ref_action<R: Rng>(&self, state: &ModelState, rng: &mut R) -> Action {
        let regions = state.regions.keys().copied().collect::<Vec<_>>();
        if regions.is_empty() {
            return Action::CreateRegion {
                id: 1,
                role: Role::Leader,
            };
        }

        let idx = rng.random_range(0..regions.len());
        let region = regions[idx];
        let file = self.pick_or_new_file_ref(state, rng);

        match rng.random_range(0..4u8) {
            0 => Action::AddTempRef { region, file },
            1 => Action::RemoveTempRef { region, file },
            2 => {
                let dst = regions[rng.random_range(0..regions.len())];
                Action::AddCrossRegionRef {
                    src_region: region,
                    dst_region: dst,
                }
            }
            _ => {
                let dst = regions[rng.random_range(0..regions.len())];
                Action::RemoveCrossRegionRef {
                    src_region: region,
                    dst_region: dst,
                }
            }
        }
    }

    fn sample_file_or_topology_action<R: Rng>(&self, state: &ModelState, rng: &mut R) -> Action {
        let regions = state.regions.keys().copied().collect::<Vec<_>>();
        if regions.is_empty() {
            return Action::CreateRegion {
                id: 1,
                role: Role::Leader,
            };
        }

        let region = regions[rng.random_range(0..regions.len())];
        let file = self.pick_or_new_file_ref(state, rng);

        match rng.random_range(0..8u8) {
            0 => Action::CreateRegion {
                id: rng.random_range(1..=(self.max_regions as u64).max(1)),
                role: if rng.random_bool(0.5) {
                    Role::Leader
                } else {
                    Role::Follower
                },
            },
            1 => Action::DropRegion { id: region },
            2 => Action::PromoteFollower { id: region },
            3 => Action::DemoteLeader { id: region },
            4..=5 => Action::CreateFileInManifest { region, file },
            _ => Action::MoveFileToRemoved { region, file },
        }
    }

    fn pick_or_new_file_ref<R: Rng>(&self, state: &ModelState, rng: &mut R) -> FileRef {
        if !state.all_files.is_empty() && rng.random_bool(0.7) {
            let files = state.all_files.iter().copied().collect::<Vec<_>>();
            files[rng.random_range(0..files.len())]
        } else {
            FileRef {
                file_id: rng.random(),
                index_version: if rng.random_bool(0.3) {
                    Some(rng.random_range(0..8))
                } else {
                    None
                },
            }
        }
    }

    fn has_follower_cross_region_pressure(&self, state: &ModelState) -> bool {
        let has_follower = state
            .regions
            .values()
            .any(|region| region.role == Role::Follower);
        let has_cross_region_ref = state
            .temp_refs
            .cross_region_refs
            .values()
            .any(|refs| !refs.is_empty());

        has_follower && has_cross_region_ref
    }

    fn has_liveness_pressure(&self, state: &ModelState) -> bool {
        state.regions.values().any(|region| {
            region.removed_files.iter().any(|(removed_at, files)| {
                !files.is_empty() && *removed_at + state.lingering_millis <= state.now + 1000
            })
        })
    }

    fn has_manifest_activity(&self, state: &ModelState) -> bool {
        state
            .regions
            .values()
            .any(|region| region.manifest_version > region.previous_manifest_version)
    }
}
