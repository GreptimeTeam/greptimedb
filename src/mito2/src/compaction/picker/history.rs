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

use crate::compaction::picker::PickerOutput;
use crate::compaction::twcs::PickClass;
use crate::sst::file::RegionFileId;

/// Waiting compensation starts after one minute. This is a promotion threshold,
/// not a deadline: an executing batch is never preempted to run an overdue pick.
const PROMOTION_DELAY: Duration = Duration::from_secs(60);

/// A work identity survives file replacement and picking epochs, but not a region
/// incarnation. Different classes in the same window retain independent ages.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct PickKey {
    pub(crate) window: i64,
    pub(crate) class: PickClass,
}

/// Region-resident scheduling metadata. It owns no file handles or reservations.
/// Planning only discovers/selects work; the owner acknowledges service after a
/// successful execution. Cancelled, stale and failed plans do not reset ages.
#[derive(Debug, Default)]
pub(crate) struct PickHistory {
    waiting: HashMap<PickKey, Instant>,
    selected: Vec<(PickKey, Vec<RegionFileId>)>,
}

impl PickHistory {
    /// Refreshes eligible identities without resetting surviving work. Storage
    /// is bounded by the current snapshot's windows times the three pick classes.
    pub(crate) fn observe(&mut self, keys: impl Iterator<Item = PickKey>, now: Instant) {
        let live: HashSet<_> = keys.collect();
        self.waiting.retain(|key, _| live.contains(key));
        for key in live {
            self.waiting.entry(key).or_insert(now);
        }
        self.selected.clear();
    }

    /// Overdue work precedes ordinary priority, oldest first. Normal picks retain
    /// the existing class/newest-window ordering. The caller supplies one clock
    /// reading for the entire sort, so the comparator stays consistent.
    pub(crate) fn priority(
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

    pub(crate) fn select(&mut self, key: PickKey, inputs: Vec<RegionFileId>) {
        self.selected.push((key, inputs));
    }

    /// Closure expansion and stale-plan validation may discard selected seeds.
    /// Only seeds wholly included in the accepted batch can receive service.
    pub(crate) fn accept(&mut self, output: &PickerOutput) {
        self.accept_inputs(
            output
                .outputs
                .iter()
                .flat_map(|o| o.inputs.iter().map(|file| file.file_id())),
        );
    }

    pub(crate) fn accept_inputs(&mut self, inputs: impl Iterator<Item = RegionFileId>) {
        let inputs: HashSet<_> = inputs.collect();
        self.selected
            .retain(|(_, files)| files.iter().all(|id| inputs.contains(id)));
    }

    pub(crate) fn finish_success(&mut self, now: Instant) {
        for (key, _) in self.selected.drain(..) {
            if let Some(since) = self.waiting.get_mut(&key) {
                *since = now;
            }
        }
    }

    /// A manual request does not use automatic selection and cannot acknowledge
    /// the selection of a previously failed automatic cycle.
    pub(crate) fn discard_selection(&mut self) {
        self.selected.clear();
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
            history.select(l0, Vec::new());
            history.finish_success(now);
        }
        let now = start + PROMOTION_DELAY;
        assert!(history.priority(old, now) < history.priority(l0, now));
        history.select(old, Vec::new());
        history.finish_success(now);
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

    #[test]
    fn test_unaccepted_and_cancelled_seeds_keep_their_age() {
        use crate::compaction::test_util::new_file_handle;
        use store_api::storage::FileId;

        let start = Instant::now();
        let key = key(0, PickClass::FileReduction);
        let file = new_file_handle(FileId::random(), 0, 10, 1);
        let mut history = PickHistory::default();
        history.observe([key].into_iter(), start);
        history.select(key, vec![file.file_id()]);
        history.accept(&PickerOutput::default());
        history.finish_success(start + PROMOTION_DELAY);
        assert_eq!(start, history.waiting[&key]);
        history.select(key, vec![file.file_id()]);
        history.discard_selection();
        history.finish_success(start + PROMOTION_DELAY);
        assert_eq!(start, history.waiting[&key]);
    }
}
