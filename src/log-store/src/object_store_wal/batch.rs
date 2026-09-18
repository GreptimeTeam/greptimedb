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

//! Accumulation of admitted entries into the batch that becomes the next
//! object, and the entry id scheme.

use std::collections::HashMap;
use std::time::Instant;

use snafu::ensure;
use store_api::logstore::EntryId;
use store_api::logstore::entry::Entry;
use store_api::storage::RegionId;

use crate::error::{Result, WalEntryPositionExhaustedSnafu};

/// Bits of an entry id that hold the position of the entry among the entries
/// of its region inside the object; the remaining high bits hold the object
/// sequence.
pub(crate) const POSITION_BITS: u32 = 20;
/// Positions are `1..POSITION_LIMIT`, so an object holds at most 2^20 - 1
/// entries of one region and a batch seals before a region reaches the limit.
/// This is a theoretical bound: an object is sealed by size long before.
pub(crate) const POSITION_LIMIT: u64 = 1 << POSITION_BITS;
/// Object sequences are `0..OBJECT_SEQ_LIMIT`, the 44 bits an entry id leaves
/// above the position.
pub(crate) const OBJECT_SEQ_LIMIT: u64 = 1 << (u64::BITS - POSITION_BITS);

/// Returns the id of the entry at `position` among the entries of its region
/// in the object `object_seq`.
///
/// Ids are object-sequence-major: `entry_id >> 20` names the object that holds
/// the entry, and the low bits are its position in that object, which starts
/// at one so that id zero, the watermark of a region without entries, is never
/// assigned. A region's ids increase with the object sequence and have gaps
/// wherever other regions or other positions took the sequence.
pub(crate) fn entry_id(object_seq: u64, position: u64) -> EntryId {
    debug_assert!(object_seq < OBJECT_SEQ_LIMIT && (1..POSITION_LIMIT).contains(&position));
    (object_seq << POSITION_BITS) | position
}

/// Returns the smallest object sequence whose entry ids are all greater than
/// `entry_id`. Zero names no entry, so it needs no floor.
///
/// An id assigned under the earlier contiguous scheme carries no object
/// information, but the floor keeps every new id above it just the same.
pub(crate) fn sequence_floor(entry_id: EntryId) -> u64 {
    if entry_id == 0 {
        0
    } else {
        (entry_id >> POSITION_BITS) + 1
    }
}

/// Entries admitted since the last seal, together with the position of the
/// last entry admitted per region.
///
/// Every entry is assigned its id at admission from the sequence the batch
/// takes when it is sealed, so a batch that is rolled back and admitted again
/// under the same sequence hands out the same ids.
#[derive(Debug)]
pub(crate) struct OpenBatch {
    max_bytes: usize,
    entries: Vec<Entry>,
    estimated_bytes: usize,
    /// When the first entry of the batch was admitted.
    first_admitted_at: Option<Instant>,
    positions: HashMap<RegionId, u64>,
}

impl OpenBatch {
    pub(crate) fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            entries: Vec::new(),
            estimated_bytes: 0,
            first_admitted_at: None,
            positions: HashMap::new(),
        }
    }

    /// Returns true when admitting `entries` would take a region past the
    /// position range, so that `entries` need a batch of their own.
    pub(crate) fn would_exhaust_positions(&self, entries: &[Entry]) -> bool {
        self.check_positions(entries).is_err()
    }

    /// Admits `entries` into the batch that becomes the object `object_seq`,
    /// assigning each the next position of its region, and returns the last id
    /// assigned to every region in `entries`. Nothing is admitted when a
    /// region would run past the position range.
    pub(crate) fn admit(
        &mut self,
        object_seq: u64,
        mut entries: Vec<Entry>,
    ) -> Result<HashMap<RegionId, EntryId>> {
        self.check_positions(&entries)?;
        let mut last_entry_ids = HashMap::new();
        for entry in &mut entries {
            let region_id = entry.region_id();
            let position = self.positions.entry(region_id).or_insert(0);
            *position += 1;
            let entry_id = entry_id(object_seq, *position);
            entry.set_entry_id(entry_id);
            last_entry_ids.insert(region_id, entry_id);
        }
        self.estimated_bytes += entries.iter().map(Entry::estimated_size).sum::<usize>();
        if !entries.is_empty() {
            self.first_admitted_at.get_or_insert_with(Instant::now);
        }
        self.entries.extend(entries);
        Ok(last_entry_ids)
    }

    /// Checks that every region in `entries` stays inside the position range
    /// once they are admitted.
    fn check_positions(&self, entries: &[Entry]) -> Result<()> {
        let mut positions = HashMap::new();
        for entry in entries {
            let region_id = entry.region_id();
            let position = positions
                .entry(region_id)
                .or_insert_with(|| self.positions.get(&region_id).copied().unwrap_or(0));
            *position += 1;
            ensure!(
                *position < POSITION_LIMIT,
                WalEntryPositionExhaustedSnafu { region_id }
            );
        }
        Ok(())
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns the estimated size of the admitted entries.
    pub(crate) fn estimated_bytes(&self) -> usize {
        self.estimated_bytes
    }

    /// Returns when the first admitted entry was admitted, if any.
    pub(crate) fn first_admitted_at(&self) -> Option<Instant> {
        self.first_admitted_at
    }

    /// Returns true once the admitted entries reach the size limit.
    pub(crate) fn should_seal(&self) -> bool {
        !self.is_empty() && self.estimated_bytes >= self.max_bytes
    }

    /// Takes the admitted entries out of the batch together with the time the
    /// first of them was admitted. The next admission starts at position one
    /// again, under the next sequence.
    pub(crate) fn seal(&mut self) -> (Vec<Entry>, Instant) {
        self.estimated_bytes = 0;
        self.positions.clear();
        let first_admitted_at = self.first_admitted_at.take().unwrap_or_else(Instant::now);
        (std::mem::take(&mut self.entries), first_admitted_at)
    }

    /// Drops the admitted entries, so the next admission under the same
    /// sequence hands out the same ids again.
    pub(crate) fn reset(&mut self) {
        let _ = self.seal();
    }
}

#[cfg(test)]
mod tests {
    use store_api::logstore::entry::NaiveEntry;
    use store_api::logstore::provider::Provider;

    use super::*;
    use crate::error::Error;

    fn entry(region_id: RegionId, payload_len: usize) -> Entry {
        Entry::Naive(NaiveEntry {
            provider: Provider::object_store_provider(region_id, "wal".to_string()),
            region_id,
            entry_id: 0,
            data: vec![0; payload_len],
        })
    }

    fn entry_ids(entries: &[Entry]) -> Vec<(RegionId, EntryId)> {
        entries
            .iter()
            .map(|entry| (entry.region_id(), entry.entry_id()))
            .collect()
    }

    #[test]
    fn test_entry_id_scheme() {
        assert_eq!(1, entry_id(0, 1));
        assert_eq!(0x10_0001, entry_id(1, 1));
        // 7 << 52 | 3
        assert_eq!(31_525_197_391_593_475, entry_id(0x7_0000_0000, 3));
        assert_eq!(u64::MAX, entry_id(OBJECT_SEQ_LIMIT - 1, POSITION_LIMIT - 1));
        assert_eq!(1 << 44, OBJECT_SEQ_LIMIT);

        assert_eq!(0, sequence_floor(0));
        assert_eq!(1, sequence_floor(1));
        assert_eq!(1, sequence_floor(POSITION_LIMIT - 1));
        assert_eq!(2, sequence_floor(entry_id(1, 1)));
        assert_eq!(8, sequence_floor(entry_id(7, POSITION_LIMIT - 1)));
        // A contiguous id well past the first object is placed by its high bits.
        assert_eq!(5, sequence_floor(5_000_000));
        assert_eq!(OBJECT_SEQ_LIMIT, sequence_floor(u64::MAX));
    }

    #[test]
    fn test_batch_assigns_positions_per_region_under_the_object_sequence() {
        let region_a = RegionId::new(1, 1);
        let region_b = RegionId::new(1, 2);
        let mut batch = OpenBatch::new(usize::MAX);

        let first = batch
            .admit(5, vec![entry(region_a, 1), entry(region_b, 1)])
            .unwrap();
        assert_eq!(
            HashMap::from([(region_a, entry_id(5, 1)), (region_b, entry_id(5, 1))]),
            first
        );
        let second = batch
            .admit(
                5,
                vec![entry(region_b, 1), entry(region_a, 1), entry(region_b, 1)],
            )
            .unwrap();
        assert_eq!(
            HashMap::from([(region_a, entry_id(5, 2)), (region_b, entry_id(5, 3))]),
            second
        );

        assert_eq!(
            vec![
                (region_a, entry_id(5, 1)),
                (region_b, entry_id(5, 1)),
                (region_b, entry_id(5, 2)),
                (region_a, entry_id(5, 2)),
                (region_b, entry_id(5, 3)),
            ],
            entry_ids(&batch.seal().0)
        );
        assert!(batch.is_empty());
        assert_eq!(
            HashMap::from([(region_a, entry_id(6, 1))]),
            batch.admit(6, vec![entry(region_a, 1)]).unwrap()
        );
        assert_eq!(POSITION_LIMIT, entry_id(6, 1) - entry_id(5, 1));
    }

    #[test]
    fn test_batch_seals_at_size_limit() {
        let region_id = RegionId::new(1, 1);
        let first = entry(region_id, 8);
        let second = entry(region_id, 8);
        let max_bytes = first.estimated_size() + second.estimated_size();
        let mut batch = OpenBatch::new(max_bytes);

        assert!(!batch.should_seal());
        batch.admit(0, vec![first]).unwrap();
        assert!(!batch.should_seal());
        batch.admit(0, vec![second]).unwrap();
        assert!(batch.should_seal());

        assert_eq!(2, batch.seal().0.len());
        assert!(!batch.should_seal());
    }

    #[test]
    fn test_batch_admission_clock_starts_with_the_first_entry() {
        let region_id = RegionId::new(1, 1);
        let mut batch = OpenBatch::new(usize::MAX);

        assert!(batch.admit(0, Vec::new()).unwrap().is_empty());
        assert!(batch.is_empty());
        assert_eq!(None, batch.first_admitted_at());

        let before = Instant::now();
        batch.admit(0, vec![entry(region_id, 1)]).unwrap();
        let first_admitted_at = batch.first_admitted_at().unwrap();
        assert!(first_admitted_at >= before);
        batch.admit(0, vec![entry(region_id, 1)]).unwrap();
        assert_eq!(Some(first_admitted_at), batch.first_admitted_at());
        assert_eq!(first_admitted_at, batch.seal().1);
        assert_eq!(None, batch.first_admitted_at());
    }

    #[test]
    fn test_batch_reset_hands_out_the_same_ids_again() {
        let region_id = RegionId::new(1, 1);
        let mut batch = OpenBatch::new(usize::MAX);

        assert_eq!(
            HashMap::from([(region_id, entry_id(3, 1))]),
            batch.admit(3, vec![entry(region_id, 1)]).unwrap()
        );
        batch.reset();
        assert!(batch.is_empty());
        assert_eq!(
            HashMap::from([(region_id, entry_id(3, 1))]),
            batch.admit(3, vec![entry(region_id, 1)]).unwrap()
        );
    }

    #[test]
    fn test_batch_refuses_a_region_past_the_position_range() {
        let region_a = RegionId::new(1, 1);
        let region_b = RegionId::new(1, 2);
        let mut batch = OpenBatch::new(usize::MAX);
        let entries =
            |region_id, count: u64| (0..count).map(|_| entry(region_id, 0)).collect::<Vec<_>>();

        // An append that alone runs past the range fits no object.
        let error = batch
            .admit(0, entries(region_a, POSITION_LIMIT))
            .unwrap_err();
        assert!(
            matches!(error, Error::WalEntryPositionExhausted { region_id, .. } if region_id == region_a),
            "unexpected error: {error:?}"
        );
        assert!(batch.is_empty());

        // The range holds one entry fewer; the next entry of that region needs
        // a new batch, while another region still fits.
        let last = batch
            .admit(0, entries(region_a, POSITION_LIMIT - 1))
            .unwrap();
        assert_eq!(
            HashMap::from([(region_a, entry_id(0, POSITION_LIMIT - 1))]),
            last
        );
        assert!(batch.would_exhaust_positions(&entries(region_a, 1)));
        assert!(!batch.would_exhaust_positions(&entries(region_b, 1)));
        let error = batch.admit(0, entries(region_a, 1)).unwrap_err();
        assert!(
            matches!(error, Error::WalEntryPositionExhausted { .. }),
            "unexpected error: {error:?}"
        );
        assert_eq!(POSITION_LIMIT as usize - 1, batch.seal().0.len());
        assert_eq!(
            HashMap::from([(region_a, entry_id(1, 1))]),
            batch.admit(1, entries(region_a, 1)).unwrap()
        );
    }
}
