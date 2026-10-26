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

use std::cmp::Reverse;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use crate::compaction::twcs::{PickClass, WindowCandidate};
use crate::metrics::TWCS_SELECTED_CANDIDATES;

/// Waiting compensation starts after one minute. This is a promotion threshold,
/// not a deadline: an executing batch is never preempted to run an overdue pick.
/// It is checked at pick time and does not wake the scheduler on its own.
const PROMOTION_DELAY: Duration = Duration::from_secs(60);

/// A work identity survives file replacement and picking epochs, but not a region
/// incarnation. Different classes in the same window retain independent ages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PickKey {
    /// Window identity, independent of the current input files.
    pub(crate) window: i64,
    /// Work category whose waiting age is tracked independently in this window.
    pub(crate) class: PickClass,
}

/// Region-resident scheduling metadata. It owns no file handles or reservations.
/// Planning works on a private snapshot; the accepted plan consumes one fairness
/// turn per selected key. Execution success or failure does not rewrite ages.
#[derive(Debug, Default, Clone)]
pub(crate) struct PickHistory {
    /// First observation or last fairness turn for each eligible key.
    /// Repeated picks preserve this timestamp until the key is selected.
    waiting: HashMap<PickKey, Instant>,
}

impl PickHistory {
    /// Refreshes all eligible identities before selection limits hide any work.
    /// Preserves surviving ages and drops absent keys without changing selection.
    fn observe(&mut self, keys: impl Iterator<Item = PickKey>, now: Instant) {
        let live: HashSet<_> = keys.collect();
        self.waiting.retain(|key, _| live.contains(key));
        for key in live {
            self.waiting.entry(key).or_insert(now);
        }
    }

    /// Returns an ascending sort key: promoted work first, oldest waiting first.
    /// Below [`PROMOTION_DELAY`], age does not affect class/newest-window ordering.
    /// One shared `now` keeps the comparator consistent across the entire sort.
    fn priority(
        &self,
        key: PickKey,
        now: Instant,
    ) -> (bool, Option<Instant>, PickClass, Reverse<i64>) {
        let overdue = self
            .waiting
            .get(&key)
            .copied()
            .filter(|since| now.saturating_duration_since(*since) >= PROMOTION_DELAY);
        (overdue.is_none(), overdue, key.class, Reverse(key.window))
    }

    /// Consumes the fairness turn for `key`. Only an accepted plan installs the
    /// private snapshot, so rejected or stale plans never advance resident ages.
    fn serve(&mut self, key: PickKey, now: Instant) {
        self.waiting.insert(key, now);
    }

    /// Applies region fairness to a complete discovery result. Planning uses a
    /// private snapshot; the worker installs it only after validating the plan ID.
    /// Each selected key consumes its fairness turn here, so a plan that never
    /// completes cannot keep a promoted key at the front. Keeps at most one
    /// alternative per window, then applies `max_outputs`.
    pub(crate) fn select_candidates(
        &mut self,
        mut candidates: Vec<WindowCandidate>,
        max_outputs: Option<usize>,
        now: Instant,
    ) -> Vec<WindowCandidate> {
        let key = |candidate: &WindowCandidate| PickKey {
            window: candidate.window,
            class: candidate.pick.class,
        };
        self.observe(candidates.iter().map(key), now);
        candidates.sort_by_key(|candidate| self.priority(key(candidate), now));
        // Alternatives may share files or semantic dependencies within a window.
        let mut windows = HashSet::new();
        candidates.retain(|candidate| windows.insert(candidate.window));
        candidates.truncate(max_outputs.unwrap_or(usize::MAX));
        for candidate in &candidates {
            let key = key(candidate);
            let priority = if self.priority(key, now).1.is_some() {
                "promoted"
            } else {
                "normal"
            };
            TWCS_SELECTED_CANDIDATES
                .with_label_values(&[key.class.as_str(), priority])
                .inc();
            self.serve(key, now);
        }
        candidates
    }
}

#[cfg(test)]
impl PickHistory {
    /// Seeds waiting ages for cross-module scheduling tests.
    pub(crate) fn observe_for_test(&mut self, keys: impl Iterator<Item = PickKey>, now: Instant) {
        self.observe(keys, now);
    }

    /// Seeds one waiting age without pruning unrelated keys.
    pub(crate) fn seed_for_test(&mut self, key: PickKey, now: Instant) {
        self.serve(key, now);
    }

    /// Inspects ordering without exposing the production policy helper.
    pub(crate) fn priority_for_test(
        &self,
        key: PickKey,
        now: Instant,
    ) -> (bool, Option<Instant>, PickClass, Reverse<i64>) {
        self.priority(key, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(window: i64, class: PickClass) -> PickKey {
        PickKey { window, class }
    }

    #[test]
    fn test_aging_survives_repicks_and_other_service() {
        let start = Instant::now();
        let l0 = key(100, PickClass::L0Merge);
        let old = key(0, PickClass::FileReduction);
        let overlap = key(0, PickClass::OverlapResolution);
        let mut history = PickHistory::default();
        history.observe([l0, old, overlap].into_iter(), start);
        assert!(history.priority(l0, start) < history.priority(old, start));
        for seconds in 1..=60 {
            let now = start + Duration::from_secs(seconds);
            history.observe([l0, old, overlap].into_iter(), now);
            history.serve(l0, now);
        }
        let now = start + PROMOTION_DELAY;
        assert!(history.priority(old, now) < history.priority(l0, now));
        history.serve(old, now);
        assert!(history.priority(overlap, now) < history.priority(old, now));
    }

    #[test]
    fn test_old_windows_progress_and_obsolete_work_is_pruned() {
        let start = Instant::now();
        let old = key(0, PickClass::L0Merge);
        let new = key(1, PickClass::L0Merge);
        let mut history = PickHistory::default();
        history.observe([old].into_iter(), start);
        let now = start + PROMOTION_DELAY;
        history.observe([old, new].into_iter(), now);
        assert!(history.priority(old, now) < history.priority(new, now));
        history.observe([new].into_iter(), now);
        assert_eq!(1, history.waiting.len());
        assert!(!history.waiting.contains_key(&old));
        history.observe(std::iter::empty(), now);
        assert!(history.waiting.is_empty());
    }
}
