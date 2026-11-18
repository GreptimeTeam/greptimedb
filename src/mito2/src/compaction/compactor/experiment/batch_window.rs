// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! One immutable source roster per round. Position determines the batch on every DN.
use std::collections::HashSet;
use std::hash::Hash;

pub(super) fn new_roster<T>(
    ordered: impl Iterator<Item = T>,
    width: usize,
    slots: usize,
) -> Vec<T> {
    let mut files: Vec<_> = ordered.take(width.saturating_mul(slots)).collect();
    files.truncate(files.len() / width * width);
    files
}

pub(super) fn valid_roster<T: Eq + Hash>(files: &[T], width: usize) -> bool {
    !files.is_empty()
        && files.len().is_multiple_of(width)
        && files.iter().collect::<HashSet<_>>().len() == files.len()
}

pub(super) fn batch_for_unit<'a, T: Eq + Hash>(
    roster: &'a [T],
    width: usize,
    reserved: &HashSet<&T>,
) -> Option<(usize, &'a [T])> {
    if reserved.len() != width || !valid_roster(roster, width) {
        return None;
    }
    roster
        .chunks_exact(width)
        .enumerate()
        .find(|(_, batch)| batch.iter().all(|id| reserved.contains(id)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disjoint_groups_and_retry_keep_identical_ownership() {
        for slots in [1, 8, 16, 24, 48] {
            let roster = new_roster(0..2053, 32, slots);
            let mut completed = HashSet::new();
            for (index, ids) in roster.chunks_exact(32).enumerate().rev() {
                let unit = ids.iter().rev().collect();
                assert_eq!(batch_for_unit(&roster, 32, &unit), Some((index, ids)));
                assert!(ids.iter().all(|id| completed.insert(*id)));
                assert_eq!(batch_for_unit(&roster, 32, &unit), Some((index, ids)));
            }
            assert_eq!(completed.len(), slots * 32);
            let next = new_roster((0..2053).filter(|id| !completed.contains(id)), 32, slots);
            assert!(next.iter().all(|id| !completed.contains(id)));
        }
    }

    #[test]
    fn reject_partial_mixed_and_duplicate_reservations() {
        let roster = new_roster(0..9, 4, 16);
        assert_eq!(roster, (0..8).collect::<Vec<_>>());
        assert!(new_roster(0..3, 4, 16).is_empty());
        for invalid in [&roster[1..5], &roster[..3]] {
            assert!(batch_for_unit(&roster, 4, &invalid.iter().collect()).is_none());
        }
        let duplicate = [0, 1, 2, 3, 3, 5, 6, 7];
        assert!(batch_for_unit(&duplicate, 4, &roster[..4].iter().collect()).is_none());
    }
}
