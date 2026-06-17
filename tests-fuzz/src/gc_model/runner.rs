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

use std::collections::HashSet;

use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;

use crate::gc_model::action::{Action, ActionGenerator, GcMode};
use crate::gc_model::oracle::{
    assert_liveness_progress, assert_mode_monotonicity, assert_safety_follower,
    assert_safety_manifest, assert_safety_reachable, expected_deletable, reachable_files,
};
use crate::gc_model::state::{
    FileRef, Lifecycle, ModelState, ObservedRegionState, ObservedSnapshot, RegionId, RegionState,
    Role,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultInjection {
    pub ignore_temp_refs: bool,
    pub ignore_follower_protection: bool,
    pub ignore_cross_region_refs: bool,
    pub ignore_lingering: bool,
}

#[derive(Clone, Debug)]
pub struct FuzzInput {
    pub seed: u64,
    pub actions: usize,
    pub max_regions: u8,
    pub linger_ms: u32,
    pub mismatch_rate: u8,
    pub fault_injection: FaultInjection,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RegressionSeedConfig {
    pub name: &'static str,
    pub seed: u64,
    pub actions: usize,
    pub max_regions: u8,
    pub linger_ms: u32,
    pub mismatch_rate: u8,
    pub fault_injection: FaultInjection,
    pub note: &'static str,
}

impl RegressionSeedConfig {
    pub const fn as_input(self) -> FuzzInput {
        FuzzInput {
            seed: self.seed,
            actions: self.actions,
            max_regions: self.max_regions,
            linger_ms: self.linger_ms,
            mismatch_rate: self.mismatch_rate,
            fault_injection: self.fault_injection,
        }
    }
}

const REGRESSION_SMOKE_CASES: [RegressionSeedConfig; 4] = [
    RegressionSeedConfig {
        name: "reachability-seed-42",
        seed: 42,
        actions: 128,
        max_regions: 12,
        linger_ms: 50,
        mismatch_rate: 30,
        fault_injection: FaultInjection {
            ignore_temp_refs: false,
            ignore_follower_protection: false,
            ignore_cross_region_refs: false,
            ignore_lingering: false,
        },
        note: "Retained because it previously exposed the reachable propagation split.",
    },
    RegressionSeedConfig {
        name: "high-mismatch-seed-7",
        seed: 7,
        actions: 64,
        max_regions: 8,
        linger_ms: 200,
        mismatch_rate: 100,
        fault_injection: FaultInjection {
            ignore_temp_refs: false,
            ignore_follower_protection: false,
            ignore_cross_region_refs: false,
            ignore_lingering: false,
        },
        note: "Retained as a full mismatch pressure smoke case.",
    },
    RegressionSeedConfig {
        name: "manifest-mismatch-seed-99",
        seed: 99,
        actions: 192,
        max_regions: 16,
        linger_ms: 1,
        mismatch_rate: 80,
        fault_injection: FaultInjection {
            ignore_temp_refs: false,
            ignore_follower_protection: false,
            ignore_cross_region_refs: false,
            ignore_lingering: false,
        },
        note: "Retained because it previously exposed mismatch + manifest protection gaps.",
    },
    RegressionSeedConfig {
        name: "long-linger-seed-123456",
        seed: 123456,
        actions: 256,
        max_regions: 16,
        linger_ms: 5000,
        mismatch_rate: 60,
        fault_injection: FaultInjection {
            ignore_temp_refs: false,
            ignore_follower_protection: false,
            ignore_cross_region_refs: false,
            ignore_lingering: false,
        },
        note: "Retained as a long-lingering pressure baseline.",
    },
];

pub fn regression_smoke_cases() -> &'static [RegressionSeedConfig] {
    &REGRESSION_SMOKE_CASES
}

#[derive(Clone, Debug)]
pub struct TraceStep {
    pub idx: usize,
    pub action: Action,
    pub summary: String,
}

pub fn run(input: FuzzInput) {
    let _ = run_with_trace(input, |_| {});
}

#[derive(Clone, Debug)]
pub struct ReplaySnapshot {
    pub now: i64,
    pub region_count: usize,
    pub live_region_count: usize,
    pub manifest_file_count: usize,
    pub removed_file_count: usize,
    pub temp_ref_count: usize,
    pub cross_region_edge_count: usize,
    pub total_file_count: usize,
    pub mismatch_region_count: usize,
}

#[derive(Clone, Debug)]
pub struct ReplayStep {
    pub idx: usize,
    pub action: Action,
    pub summary: String,
    pub snapshot: ReplaySnapshot,
}

pub fn collect_replay(input: FuzzInput) -> Vec<ReplayStep> {
    let mut replay = Vec::with_capacity(input.actions);
    let _ = run_with_trace(input, |step| replay.push(step));
    replay
}

fn run_with_trace<F>(input: FuzzInput, mut on_step: F) -> ModelState
where
    F: FnMut(ReplayStep),
{
    let mut rng = ChaCha20Rng::seed_from_u64(input.seed);
    let mut state = ModelState::new(input.linger_ms as i64);
    bootstrap_state(&mut state, &mut rng, input.max_regions.max(2));

    let generator = ActionGenerator {
        max_regions: input.max_regions.max(1),
    };
    let mut trace = Vec::with_capacity(input.actions);

    for idx in 0..input.actions {
        let action = generator.sample(&state, &mut rng);
        let (summary, mismatch_region_count) = apply_action_and_validate(
            &mut state,
            &action,
            &mut rng,
            input.mismatch_rate.min(100),
            input.fault_injection,
            input.seed,
            &trace,
            idx,
        );

        trace.push(TraceStep {
            idx,
            action,
            summary,
        });

        let current = trace.last().expect("trace just pushed");
        on_step(ReplayStep {
            idx: current.idx,
            action: current.action.clone(),
            summary: current.summary.clone(),
            snapshot: snapshot_state(&state, mismatch_region_count),
        });
    }

    state
}

fn snapshot_state(state: &ModelState, mismatch_region_count: usize) -> ReplaySnapshot {
    ReplaySnapshot {
        now: state.now,
        region_count: state.regions.len(),
        live_region_count: state
            .regions
            .values()
            .filter(|region| region.lifecycle == Lifecycle::Open)
            .count(),
        manifest_file_count: state
            .regions
            .values()
            .map(|region| region.manifest_files.len())
            .sum(),
        removed_file_count: state
            .regions
            .values()
            .map(|region| {
                region
                    .removed_files
                    .values()
                    .map(std::collections::HashSet::len)
                    .sum::<usize>()
            })
            .sum(),
        temp_ref_count: state
            .temp_refs
            .file_refs_by_region
            .values()
            .map(std::collections::HashSet::len)
            .sum(),
        cross_region_edge_count: state
            .temp_refs
            .cross_region_refs
            .values()
            .map(std::collections::HashSet::len)
            .sum(),
        total_file_count: state.all_files.len(),
        mismatch_region_count,
    }
}

fn bootstrap_state(state: &mut ModelState, rng: &mut ChaCha20Rng, max_regions: u8) {
    let region_count = rng.random_range(2..=max_regions.min(4));
    for id in 1..=region_count as u64 {
        let role = if id == 1 || rng.random_bool(0.5) {
            Role::Leader
        } else {
            Role::Follower
        };
        state.regions.insert(id, RegionState::new(role));
        state.temp_refs.manifest_version_by_region.insert(id, 0);
    }

    if !state
        .regions
        .values()
        .any(|region| region.role == Role::Follower)
        && let Some(region) = state.regions.get_mut(&1)
    {
        region.role = Role::Follower;
    }
}

#[allow(clippy::too_many_arguments)]
fn apply_action_and_validate(
    state: &mut ModelState,
    action: &Action,
    rng: &mut ChaCha20Rng,
    mismatch_rate: u8,
    fault_injection: FaultInjection,
    seed: u64,
    trace: &[TraceStep],
    idx: usize,
) -> (String, usize) {
    match action {
        Action::RunGc { mode } => run_gc_step(
            state,
            *mode,
            rng,
            mismatch_rate,
            fault_injection,
            seed,
            trace,
            idx,
        ),
        _ => {
            apply_non_gc_action(state, action);
            ("state-updated".to_string(), 0)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_gc_step(
    state: &mut ModelState,
    mode: GcMode,
    rng: &mut ChaCha20Rng,
    mismatch_rate: u8,
    fault_injection: FaultInjection,
    seed: u64,
    trace: &[TraceStep],
    idx: usize,
) -> (String, usize) {
    let state_before = state.clone();
    let observed = build_observed_snapshot(
        &state_before,
        rng,
        if mode == GcMode::Fast {
            mismatch_rate
        } else {
            0
        },
    );
    let deleted = simulate_gc_result(&state_before, mode, &observed, fault_injection);

    let reachable = reachable_files(&state_before);
    let expected = expected_deletable(&state_before, mode);
    let reachable_conflict = assert_safety_reachable(&deleted, &reachable);
    let manifest_conflict = assert_safety_manifest(&state_before, &deleted);
    let follower_conflict = assert_safety_follower(&state_before, &deleted);
    let unexpected_deleted = deleted
        .iter()
        .copied()
        .filter(|file| !expected.contains(file))
        .collect::<Vec<_>>();
    let liveness_conflict = assert_liveness_progress(&state_before, &deleted, mode)
        .into_iter()
        .filter(|file| !file_in_mismatched_region(*file, &observed))
        .collect::<Vec<_>>();

    if !reachable_conflict.is_empty()
        || !manifest_conflict.is_empty()
        || !follower_conflict.is_empty()
        || !unexpected_deleted.is_empty()
        || !liveness_conflict.is_empty()
    {
        panic_with_trace(
            seed,
            idx,
            trace,
            &deleted,
            &expected,
            &reachable,
            &observed,
            &format!(
                "reachable_conflict={reachable_conflict:?}, manifest_conflict={manifest_conflict:?}, follower_conflict={follower_conflict:?}, unexpected_deleted={unexpected_deleted:?}, liveness_conflict={liveness_conflict:?}"
            ),
        );
    }

    if mode == GcMode::Fast {
        let full_deleted =
            simulate_gc_result(&state_before, GcMode::Full, &observed, fault_injection);
        let mode_violation = assert_mode_monotonicity(&deleted, &full_deleted);
        if !mode_violation.is_empty() {
            panic_with_trace(
                seed,
                idx,
                trace,
                &deleted,
                &expected,
                &reachable,
                &observed,
                &format!("mode_monotonicity_conflict={mode_violation:?}"),
            );
        }
    }

    apply_deletions(state, &deleted);

    (
        format!(
            "gc_deleted={} expected={} mode={mode:?} mismatched_regions={:?}",
            deleted.len(),
            expected.len(),
            observed.mismatched_regions
        ),
        observed.mismatched_regions.len(),
    )
}

fn build_observed_snapshot(
    actual: &ModelState,
    rng: &mut ChaCha20Rng,
    mismatch_rate: u8,
) -> ObservedSnapshot {
    let mut snapshot = ObservedSnapshot::from_actual(actual);

    for (region_id, region) in &actual.regions {
        let mismatch_injected = mismatch_rate > 0
            && region.manifest_version > region.previous_manifest_version
            && rng.random_range(0..100u8) < mismatch_rate;

        if mismatch_injected {
            snapshot.mismatched_regions.insert(*region_id);
            snapshot.regions.insert(
                *region_id,
                ObservedRegionState {
                    role: region.role,
                    lifecycle: region.lifecycle,
                    manifest_version: region.previous_manifest_version,
                    manifest_files: region.previous_manifest_files.clone(),
                    removed_files: region.previous_removed_files.clone(),
                },
            );
            snapshot
                .temp_refs
                .manifest_version_by_region
                .insert(*region_id, region.previous_manifest_version);
        } else {
            snapshot
                .temp_refs
                .manifest_version_by_region
                .insert(*region_id, region.manifest_version);
        }
    }

    snapshot
}

fn simulate_gc_result(
    state_before: &ModelState,
    mode: GcMode,
    observed: &ObservedSnapshot,
    fault_injection: FaultInjection,
) -> HashSet<FileRef> {
    let reachable = simulated_reachable_files(observed, fault_injection);
    let mut deleted = HashSet::new();
    let threshold = state_before.now - state_before.lingering_millis;

    for file in &state_before.all_files {
        if reachable.contains(file) {
            continue;
        }

        if is_lingering_satisfied_in_observed(*file, threshold, observed, fault_injection) {
            deleted.insert(*file);
        }
    }

    if mode == GcMode::Fast {
        deleted.retain(|file| !file_in_active_mismatched_region(*file, state_before, observed));
    }

    deleted
}

fn simulated_reachable_files(
    observed: &ObservedSnapshot,
    fault_injection: FaultInjection,
) -> HashSet<FileRef> {
    let mut reachable = HashSet::new();
    let mut queue = simulated_seed_regions(observed, fault_injection)
        .into_iter()
        .collect::<Vec<_>>();
    let mut visited = HashSet::new();

    while let Some(region_id) = queue.pop() {
        if !visited.insert(region_id) {
            continue;
        }

        if let Some(region) = observed.regions.get(&region_id)
            && region.lifecycle == Lifecycle::Open
        {
            reachable.extend(region.manifest_files.iter().copied());
        }

        if !fault_injection.ignore_temp_refs
            && let Some(temp_refs) = observed.temp_refs.file_refs_by_region.get(&region_id)
        {
            reachable.extend(temp_refs.iter().copied());
        }

        if !fault_injection.ignore_cross_region_refs
            && let Some(dsts) = observed.temp_refs.cross_region_refs.get(&region_id)
        {
            queue.extend(dsts.iter().copied());
        }
    }

    if !fault_injection.ignore_follower_protection {
        for region in observed.regions.values() {
            if region.lifecycle == Lifecycle::Open && region.role == Role::Follower {
                reachable.extend(region.manifest_files.iter().copied());
            }
        }
    }

    reachable
}

fn simulated_seed_regions(
    observed: &ObservedSnapshot,
    fault_injection: FaultInjection,
) -> HashSet<RegionId> {
    let mut seeds = observed
        .regions
        .iter()
        .filter_map(|(region_id, region)| {
            (region.lifecycle == Lifecycle::Open).then_some(*region_id)
        })
        .collect::<HashSet<_>>();

    if !fault_injection.ignore_temp_refs {
        seeds.extend(observed.temp_refs.file_refs_by_region.keys().copied());
    }

    if !fault_injection.ignore_follower_protection {
        seeds.extend(observed.regions.iter().filter_map(|(region_id, region)| {
            (region.lifecycle == Lifecycle::Open && region.role == Role::Follower)
                .then_some(*region_id)
        }));
    }

    seeds
}

fn is_lingering_satisfied_in_observed(
    file: FileRef,
    threshold: i64,
    observed: &ObservedSnapshot,
    fault_injection: FaultInjection,
) -> bool {
    observed.regions.values().any(|region| {
        region.removed_files.iter().any(|(removed_at, files)| {
            files.contains(&file) && (fault_injection.ignore_lingering || *removed_at <= threshold)
        })
    })
}

fn file_in_mismatched_region(file: FileRef, observed: &ObservedSnapshot) -> bool {
    observed
        .mismatched_regions
        .iter()
        .filter_map(|region_id| observed.regions.get(region_id))
        .any(|region| {
            region.manifest_files.contains(&file)
                || region
                    .removed_files
                    .values()
                    .any(|files| files.contains(&file))
        })
}

fn file_in_active_mismatched_region(
    file: FileRef,
    state_before: &ModelState,
    observed: &ObservedSnapshot,
) -> bool {
    file_in_mismatched_region(file, observed)
        || observed
            .mismatched_regions
            .iter()
            .filter_map(|region_id| state_before.regions.get(region_id))
            .any(|region| {
                region.manifest_files.contains(&file)
                    || region
                        .removed_files
                        .values()
                        .any(|files| files.contains(&file))
            })
}

fn apply_non_gc_action(state: &mut ModelState, action: &Action) {
    match action {
        Action::CreateRegion { id, role } => {
            state
                .regions
                .entry(*id)
                .or_insert_with(|| RegionState::new(*role));
            state
                .temp_refs
                .manifest_version_by_region
                .entry(*id)
                .or_insert(0);
        }
        Action::DropRegion { id } => {
            if let Some(region) = state.regions.get_mut(id) {
                if region.lifecycle == Lifecycle::Open {
                    region.checkpoint_manifest_view();
                    let files = region.manifest_files.iter().copied().collect::<Vec<_>>();
                    for file in files {
                        region.manifest_files.remove(&file);
                        region
                            .removed_files
                            .entry(state.now)
                            .or_default()
                            .insert(file);
                    }
                }
                region.lifecycle = Lifecycle::Dropped;
                region.manifest_version = region.manifest_version.saturating_add(1);
                state
                    .temp_refs
                    .manifest_version_by_region
                    .insert(*id, region.manifest_version);
                state.temp_refs.file_refs_by_region.remove(id);
                state.temp_refs.cross_region_refs.remove(id);
                for refs in state.temp_refs.cross_region_refs.values_mut() {
                    refs.remove(id);
                }
            }
        }
        Action::PromoteFollower { id } => {
            if let Some(region) = state.regions.get_mut(id) {
                region.role = Role::Leader;
            }
        }
        Action::DemoteLeader { id } => {
            if let Some(region) = state.regions.get_mut(id) {
                region.role = Role::Follower;
            }
        }
        Action::CreateFileInManifest { region, file } => {
            if let Some(region_state) = state.regions.get_mut(region)
                && region_state.lifecycle == Lifecycle::Open
            {
                region_state.checkpoint_manifest_view();
                region_state.manifest_files.insert(*file);
                region_state.manifest_version = region_state.manifest_version.saturating_add(1);
                state
                    .temp_refs
                    .manifest_version_by_region
                    .insert(*region, region_state.manifest_version);
                state.all_files.insert(*file);
            }
        }
        Action::MoveFileToRemoved { region, file } => {
            if let Some(region_state) = state.regions.get_mut(region)
                && region_state.lifecycle == Lifecycle::Open
                && region_state.manifest_files.contains(file)
            {
                region_state.checkpoint_manifest_view();
                region_state.manifest_files.remove(file);
                region_state
                    .removed_files
                    .entry(state.now)
                    .or_default()
                    .insert(*file);
                region_state.manifest_version = region_state.manifest_version.saturating_add(1);
                state
                    .temp_refs
                    .manifest_version_by_region
                    .insert(*region, region_state.manifest_version);
            }
        }
        Action::AddTempRef { region, file } => {
            state
                .temp_refs
                .file_refs_by_region
                .entry(*region)
                .or_default()
                .insert(*file);
            state.all_files.insert(*file);
        }
        Action::RemoveTempRef { region, file } => {
            if let Some(set) = state.temp_refs.file_refs_by_region.get_mut(region) {
                set.remove(file);
            }
        }
        Action::AddCrossRegionRef {
            src_region,
            dst_region,
        } => {
            state
                .temp_refs
                .cross_region_refs
                .entry(*src_region)
                .or_default()
                .insert(*dst_region);
        }
        Action::RemoveCrossRegionRef {
            src_region,
            dst_region,
        } => {
            if let Some(set) = state.temp_refs.cross_region_refs.get_mut(src_region) {
                set.remove(dst_region);
            }
        }
        Action::AdvanceTime { delta_ms } => {
            state.now = state.now.saturating_add(*delta_ms);
        }
        Action::RunGc { .. } => {}
    }
}

fn apply_deletions(state: &mut ModelState, deleted: &HashSet<FileRef>) {
    if deleted.is_empty() {
        return;
    }

    for region in state.regions.values_mut() {
        region.manifest_files.retain(|file| !deleted.contains(file));
        region
            .previous_manifest_files
            .retain(|file| !deleted.contains(file));
        for files in region.removed_files.values_mut() {
            files.retain(|file| !deleted.contains(file));
        }
        for files in region.previous_removed_files.values_mut() {
            files.retain(|file| !deleted.contains(file));
        }
        region.removed_files.retain(|_, files| !files.is_empty());
        region
            .previous_removed_files
            .retain(|_, files| !files.is_empty());
    }

    for refs in state.temp_refs.file_refs_by_region.values_mut() {
        refs.retain(|file| !deleted.contains(file));
    }
    state
        .temp_refs
        .file_refs_by_region
        .retain(|_, refs| !refs.is_empty());

    state.all_files.retain(|file| !deleted.contains(file));
}

#[allow(clippy::too_many_arguments)]
fn panic_with_trace(
    seed: u64,
    idx: usize,
    trace: &[TraceStep],
    deleted: &HashSet<FileRef>,
    expected: &HashSet<FileRef>,
    reachable: &HashSet<FileRef>,
    observed: &ObservedSnapshot,
    reason: &str,
) -> ! {
    let mut recent = trace.iter().rev().take(20).cloned().collect::<Vec<_>>();
    recent.reverse();
    panic!(
        "gc_model_invariant_failed: seed={seed}, action_index={idx}, reason={reason}, recent_trace={recent:?}, deleted={deleted:?}, expected={expected:?}, reachable={reachable:?}, mismatched_regions={:?}",
        observed.mismatched_regions,
    );
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap, HashSet};

    use crate::gc_model::action::GcMode;
    use crate::gc_model::oracle::reachable_files_in_snapshot;
    use crate::gc_model::runner::{
        FaultInjection, FuzzInput, collect_replay, regression_smoke_cases, run, simulate_gc_result,
        simulated_reachable_files,
    };
    use crate::gc_model::state::{
        FileRef, Lifecycle, ModelState, ObservedRegionState, ObservedSnapshot, RegionState, Role,
        TempRefsSnapshot,
    };

    #[test]
    fn test_runner_regression_smoke_suite() {
        for case in regression_smoke_cases() {
            run(case.as_input());
        }
    }

    #[test]
    fn test_collect_replay_matches_action_count() {
        let input = FuzzInput {
            seed: 11,
            actions: 16,
            max_regions: 6,
            linger_ms: 50,
            mismatch_rate: 3,
            fault_injection: FaultInjection::default(),
        };
        let replay = collect_replay(input.clone());
        assert_eq!(replay.len(), input.actions);
    }

    #[test]
    fn test_fault_injection_ignore_temp_refs_causes_bad_delete() {
        let protected = FileRef::new(1, None);
        let mut state = ModelState::new(100);
        state.now = 200;
        state.all_files.insert(protected);

        let mut region = RegionState::new(Role::Leader);
        region.removed_files.insert(0, HashSet::from([protected]));
        state.regions.insert(1, region);
        state
            .temp_refs
            .file_refs_by_region
            .insert(1, HashSet::from([protected]));

        let observed = ObservedSnapshot::from_actual(&state);
        let deleted = simulate_gc_result(
            &state,
            GcMode::Full,
            &observed,
            FaultInjection {
                ignore_temp_refs: true,
                ..FaultInjection::default()
            },
        );

        assert!(deleted.contains(&protected));
    }

    #[test]
    fn test_fault_injection_ignore_lingering_deletes_too_early() {
        let file = FileRef::new(2, None);
        let mut removed = BTreeMap::new();
        removed.insert(150, HashSet::from([file]));
        let observed = ObservedSnapshot {
            now: 200,
            regions: HashMap::from([(
                1,
                ObservedRegionState {
                    role: Role::Leader,
                    lifecycle: Lifecycle::Open,
                    manifest_version: 1,
                    manifest_files: HashSet::new(),
                    removed_files: removed,
                },
            )]),
            temp_refs: TempRefsSnapshot::default(),
            mismatched_regions: HashSet::new(),
        };

        let mut state = ModelState::new(100);
        state.now = 200;
        state.all_files.insert(file);
        state.regions.insert(1, RegionState::new(Role::Leader));

        let deleted = simulate_gc_result(
            &state,
            GcMode::Full,
            &observed,
            FaultInjection {
                ignore_lingering: true,
                ..FaultInjection::default()
            },
        );

        assert!(deleted.contains(&file));
    }

    #[test]
    fn test_simulated_reachable_matches_oracle_for_seeded_missing_region() {
        let file = FileRef::new(3, None);
        let observed = ObservedSnapshot {
            now: 0,
            regions: HashMap::new(),
            temp_refs: TempRefsSnapshot {
                manifest_version_by_region: HashMap::new(),
                file_refs_by_region: HashMap::from([(12, HashSet::from([file]))]),
                cross_region_refs: HashMap::from([(12, HashSet::from([13]))]),
            },
            mismatched_regions: HashSet::new(),
        };

        let simulated = simulated_reachable_files(&observed, FaultInjection::default());
        let expected = reachable_files_in_snapshot(&observed);

        assert_eq!(simulated, expected);
        assert!(simulated.contains(&file));
    }

    #[test]
    fn test_fast_gc_protects_current_manifest_files_in_mismatched_regions() {
        let file = FileRef::new(4, Some(0));
        let mut state = ModelState::new(1);
        state.now = 10;
        state.all_files.insert(file);

        let mut region = RegionState::new(Role::Leader);
        region.checkpoint_manifest_view();
        region.manifest_files.insert(file);
        region.manifest_version = 1;
        state.regions.insert(10, region);

        let mut observed = ObservedSnapshot::from_actual(&state);
        observed.mismatched_regions.insert(10);
        if let Some(region) = observed.regions.get_mut(&10) {
            region.manifest_version = 0;
            region.manifest_files.clear();
        }

        let deleted =
            simulate_gc_result(&state, GcMode::Fast, &observed, FaultInjection::default());
        assert!(!deleted.contains(&file));
    }
}
