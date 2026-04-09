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

use std::collections::{HashSet, VecDeque};

use crate::gc_model::action::GcMode;
use crate::gc_model::state::{FileRef, Lifecycle, ModelState, ObservedSnapshot, RegionId, Role};

pub fn reachable_files(state: &ModelState) -> HashSet<FileRef> {
    let observed = ObservedSnapshot::from_actual(state);
    reachable_files_in_snapshot(&observed)
}

pub fn reachable_files_in_snapshot(snapshot: &ObservedSnapshot) -> HashSet<FileRef> {
    let mut reachable = HashSet::new();
    let seed_regions = collect_seed_regions(snapshot);

    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    for src in seed_regions {
        queue.push_back(src);
    }

    while let Some(src) = queue.pop_front() {
        if !visited.insert(src) {
            continue;
        }
        add_region_reachability(src, snapshot, &mut reachable);

        let Some(dsts) = snapshot.temp_refs.cross_region_refs.get(&src) else {
            continue;
        };
        for dst in dsts {
            queue.push_back(*dst);
        }
    }

    for region in snapshot.regions.values() {
        if region.lifecycle == Lifecycle::Open && region.role == Role::Follower {
            reachable.extend(region.manifest_files.iter().copied());
        }
    }

    reachable
}

fn add_region_reachability(
    region_id: RegionId,
    snapshot: &ObservedSnapshot,
    reachable: &mut HashSet<FileRef>,
) {
    if let Some(region) = snapshot.regions.get(&region_id)
        && region.lifecycle == Lifecycle::Open
    {
        reachable.extend(region.manifest_files.iter().copied());
    }

    if let Some(temp_refs) = snapshot.temp_refs.file_refs_by_region.get(&region_id) {
        reachable.extend(temp_refs.iter().copied());
    }
}

fn collect_seed_regions(snapshot: &ObservedSnapshot) -> HashSet<RegionId> {
    let mut seeds = snapshot
        .regions
        .iter()
        .filter_map(|(region_id, region)| {
            (region.lifecycle == Lifecycle::Open).then_some(*region_id)
        })
        .collect::<HashSet<_>>();

    seeds.extend(snapshot.temp_refs.file_refs_by_region.keys().copied());
    seeds.extend(snapshot.regions.iter().filter_map(|(region_id, region)| {
        (region.lifecycle == Lifecycle::Open && region.role == Role::Follower).then_some(*region_id)
    }));

    seeds
}

pub fn expected_deletable(state: &ModelState, _mode: GcMode) -> HashSet<FileRef> {
    let reachable = reachable_files(state);
    let mut deletable = HashSet::new();
    let threshold = state.now - state.lingering_millis;

    for file in &state.all_files {
        if reachable.contains(file) {
            continue;
        }

        if is_lingering_satisfied(*file, threshold, state) {
            deletable.insert(*file);
        }
    }

    deletable
}

pub fn assert_liveness_progress(
    state_before: &ModelState,
    deleted: &HashSet<FileRef>,
    mode: GcMode,
) -> Vec<FileRef> {
    let expected = expected_deletable(state_before, mode);
    expected
        .into_iter()
        .filter(|file| !deleted.contains(file))
        .collect()
}

fn is_lingering_satisfied(file: FileRef, threshold: i64, state: &ModelState) -> bool {
    state.regions.values().any(|region| {
        region
            .removed_files
            .iter()
            .any(|(removed_at, files)| *removed_at <= threshold && files.contains(&file))
    })
}

pub fn assert_safety_reachable(
    deleted: &HashSet<FileRef>,
    reachable: &HashSet<FileRef>,
) -> Vec<FileRef> {
    deleted
        .iter()
        .copied()
        .filter(|f| reachable.contains(f))
        .collect()
}

pub fn assert_safety_manifest(
    state_before: &ModelState,
    deleted: &HashSet<FileRef>,
) -> Vec<FileRef> {
    let in_manifest = state_before
        .regions
        .values()
        .filter(|r| r.lifecycle == Lifecycle::Open)
        .flat_map(|r| r.manifest_files.iter().copied())
        .collect::<HashSet<_>>();

    deleted
        .iter()
        .copied()
        .filter(|f| in_manifest.contains(f))
        .collect()
}

pub fn assert_safety_follower(
    state_before: &ModelState,
    deleted: &HashSet<FileRef>,
) -> Vec<FileRef> {
    let protected = state_before
        .regions
        .values()
        .filter(|r| r.lifecycle == Lifecycle::Open && r.role == Role::Follower)
        .flat_map(|r| r.manifest_files.iter().copied())
        .collect::<HashSet<_>>();

    deleted
        .iter()
        .copied()
        .filter(|f| protected.contains(f))
        .collect()
}

pub fn assert_mode_monotonicity(
    fast_deleted: &HashSet<FileRef>,
    full_deleted: &HashSet<FileRef>,
) -> Vec<FileRef> {
    fast_deleted
        .iter()
        .copied()
        .filter(|f| !full_deleted.contains(f))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use crate::gc_model::action::GcMode;
    use crate::gc_model::oracle::{
        assert_liveness_progress, assert_mode_monotonicity, assert_safety_reachable,
        expected_deletable, reachable_files, reachable_files_in_snapshot,
    };
    use crate::gc_model::state::{FileRef, ModelState, ObservedSnapshot, RegionState, Role};

    #[test]
    fn test_reachable_from_open_region_and_temp_refs() {
        let file_a = FileRef::new(1, None);
        let file_b = FileRef::new(2, Some(1));

        let mut state = ModelState::new(1000);
        let mut region = RegionState::new(Role::Leader);
        region.manifest_files.insert(file_a);
        state.regions.insert(1, region);
        state
            .temp_refs
            .file_refs_by_region
            .insert(2, HashSet::from([file_b]));

        let reachable = reachable_files(&state);
        assert!(reachable.contains(&file_a));
        assert!(reachable.contains(&file_b));
    }

    #[test]
    fn test_expected_deletable_requires_lingering() {
        let file_a = FileRef::new(1, None);
        let mut state = ModelState::new(1000);
        state.all_files.insert(file_a);

        let mut region = RegionState::new(Role::Leader);
        region.removed_files.insert(900, HashSet::from([file_a]));
        state.regions.insert(1, region);
        state.now = 1500;

        assert!(expected_deletable(&state, GcMode::Fast).is_empty());

        state.now = 2000;
        let deletable = expected_deletable(&state, GcMode::Full);
        assert!(deletable.contains(&file_a));
    }

    #[test]
    fn test_monotonicity_helper() {
        let file_a = FileRef::new(1, None);
        let file_b = FileRef::new(2, None);
        let fast = HashSet::from([file_a, file_b]);
        let full = HashSet::from([file_a]);
        let violation = assert_mode_monotonicity(&fast, &full);
        assert_eq!(violation, vec![file_b]);
    }

    #[test]
    fn test_safety_reachable_helper() {
        let file_a = FileRef::new(1, None);
        let deleted = HashSet::from([file_a]);
        let reachable = HashSet::from([file_a]);
        assert_eq!(assert_safety_reachable(&deleted, &reachable), vec![file_a]);
    }

    #[test]
    fn test_liveness_progress_helper() {
        let file_a = FileRef::new(1, None);
        let mut state = ModelState::new(100);
        state.all_files.insert(file_a);
        let mut region = RegionState::new(Role::Leader);
        region.removed_files.insert(0, HashSet::from([file_a]));
        state.regions.insert(1, region);
        state.now = 150;

        let violation = assert_liveness_progress(&state, &HashSet::new(), GcMode::Full);
        assert_eq!(violation, vec![file_a]);
    }

    #[test]
    fn test_reachable_snapshot_ignores_dropped_seed_region() {
        let file_a = FileRef::new(1, None);
        let file_b = FileRef::new(2, None);
        let mut state = ModelState::new(100);

        let mut region_1 = RegionState::new(Role::Leader);
        region_1.lifecycle = crate::gc_model::state::Lifecycle::Dropped;
        region_1.manifest_files.insert(file_a);

        let mut region_2 = RegionState::new(Role::Leader);
        region_2.manifest_files.insert(file_b);

        state.regions.insert(1, region_1);
        state.regions.insert(2, region_2);
        state
            .temp_refs
            .cross_region_refs
            .insert(1, HashSet::from([2]));

        let snapshot = ObservedSnapshot::from_actual(&state);
        let reachable = reachable_files_in_snapshot(&snapshot);
        assert!(!reachable.contains(&file_a));
        assert!(reachable.contains(&file_b));
    }
}
