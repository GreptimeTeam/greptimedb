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

//! In-memory index over the footers of the objects of one WAL prefix.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound::{Excluded, Included, Unbounded};

use snafu::{OptionExt, ensure};
use store_api::logstore::EntryId;
use store_api::storage::RegionId;

use crate::error::{
    CorruptedWalObjectSnafu, InvalidWalEntryRangeSnafu, Result, WalObjectSequenceExhaustedSnafu,
};
use crate::object_store_wal::batch::{OBJECT_SEQ_LIMIT, sequence_floor};
use crate::object_store_wal::format::{FooterEntry, RELEASE_REGION_ID};

/// Indexes objects by sequence and, per region, the objects that hold entries
/// of that region, and counts the live segments of every object under the
/// obsolete watermarks.
#[derive(Debug, Default)]
pub(crate) struct ObjectCatalog {
    objects: BTreeMap<u64, Vec<FooterEntry>>,
    /// The objects that hold a segment of the region, as the sequence and
    /// the footer entry of the segment, keyed by the largest entry id of the
    /// segment. The entry ranges of a region increase strictly with the
    /// object sequence, so this is also sequence order.
    regions: BTreeMap<RegionId, BTreeMap<EntryId, (u64, FooterEntry)>>,
    /// Largest entry id ever indexed per region, kept after the object that
    /// held it was removed.
    max_entry_ids: HashMap<RegionId, EntryId>,
    /// The obsolete watermark per region, see [`obsolete`](Self::obsolete).
    watermarks: HashMap<RegionId, EntryId>,
    /// The number of live segments of every indexed object that holds one,
    /// see [`reclaim_boundary`](Self::reclaim_boundary).
    live_segments: BTreeMap<u64, usize>,
}

impl ObjectCatalog {
    /// Indexes the footer of the object `object_seq`. Objects may be inserted
    /// in any order, which lets recovery index them as it discovers them.
    /// Inserting a sequence that is already indexed is rejected, whether or not
    /// the footer matches the indexed one. An object without segments holds no
    /// entries but still takes its sequence.
    pub(crate) fn insert_object(
        &mut self,
        object_seq: u64,
        mut footer: Vec<FooterEntry>,
    ) -> Result<()> {
        footer.sort_unstable_by_key(|entry| entry.region_id);
        for entries in footer.windows(2) {
            ensure!(
                entries[0].region_id != entries[1].region_id,
                CorruptedWalObjectSnafu {
                    reason: format!(
                        "object {object_seq} has duplicate footer entries for region {}",
                        entries[0].region_id
                    ),
                }
            );
        }
        for entry in &footer {
            ensure!(
                entry.entry_count > 0 && entry.min_entry_id <= entry.max_entry_id,
                CorruptedWalObjectSnafu {
                    reason: format!(
                        "object {object_seq} has invalid entry range {}..={} with {} entries for region {}",
                        entry.min_entry_id, entry.max_entry_id, entry.entry_count, entry.region_id
                    ),
                }
            );
        }

        // Object sequences are unique: recovery indexes every listed key once, and
        // accepting a repeated insertion would hide a caller that lost track of it.
        // Retrying an identical write stays an object store concern.
        ensure!(
            !self.objects.contains_key(&object_seq),
            CorruptedWalObjectSnafu {
                reason: format!("object {object_seq} is already indexed"),
            }
        );

        // Validate every region before mutating either index so insertion is atomic.
        // While the indexed ranges of a region increase with the sequence, a
        // segment can only conflict with the segments right below and right
        // above its largest entry id: each must lie on the side of it that
        // its sequence is on.
        for entry in &footer {
            let Some(region_objects) = self.regions.get(&entry.region_id) else {
                continue;
            };
            let below = region_objects.range(..entry.max_entry_id).next_back();
            let above = region_objects.range(entry.max_entry_id..).next();
            for (_, (neighbor_seq, neighbor)) in below.into_iter().chain(above) {
                let ((lower_seq, lower), (upper_seq, upper)) = if *neighbor_seq < object_seq {
                    ((*neighbor_seq, neighbor), (object_seq, entry))
                } else {
                    ((object_seq, entry), (*neighbor_seq, neighbor))
                };
                ensure!(
                    lower.max_entry_id < upper.min_entry_id,
                    CorruptedWalObjectSnafu {
                        reason: out_of_order_reason(
                            entry.region_id,
                            lower_seq,
                            lower.max_entry_id,
                            upper_seq,
                            upper.min_entry_id
                        ),
                    }
                );
            }
        }

        let live = footer.iter().filter(|entry| self.is_live(entry)).count();
        if live > 0 {
            self.live_segments.insert(object_seq, live);
        }
        for entry in &footer {
            self.regions
                .entry(entry.region_id)
                .or_default()
                .insert(entry.max_entry_id, (object_seq, entry.clone()));
            self.max_entry_ids
                .entry(entry.region_id)
                .and_modify(|max| *max = (*max).max(entry.max_entry_id))
                .or_insert(entry.max_entry_id);
        }
        self.objects.insert(object_seq, footer);
        Ok(())
    }

    /// Removes the object `object_seq` from the index once it was deleted and
    /// returns the footer it held, or `None` if it was not indexed. The
    /// largest entry id of every region it held is kept.
    pub(crate) fn remove_object(&mut self, object_seq: u64) -> Option<Vec<FooterEntry>> {
        let footer = self.objects.remove(&object_seq)?;
        self.live_segments.remove(&object_seq);
        for entry in &footer {
            if let Some(objects) = self.regions.get_mut(&entry.region_id) {
                objects.remove(&entry.max_entry_id);
                if objects.is_empty() {
                    self.regions.remove(&entry.region_id);
                }
            }
        }
        Some(footer)
    }

    /// Returns true while the object `object_seq` is indexed.
    pub(crate) fn contains_object(&self, object_seq: u64) -> bool {
        self.objects.contains_key(&object_seq)
    }

    /// Moves the obsolete watermark of `region_id` up to `entry_id`; it never
    /// moves down. The segments of the region whose largest entry id the
    /// watermark reaches stop being live, so a move costs a lookup per
    /// segment it reaches, however many segments their objects hold.
    pub(crate) fn obsolete(&mut self, region_id: RegionId, entry_id: EntryId) {
        let previous = self.watermarks.get(&region_id).copied();
        if previous.is_some_and(|previous| previous >= entry_id) {
            return;
        }
        self.watermarks.insert(region_id, entry_id);
        if region_id == RELEASE_REGION_ID {
            return;
        }
        let Some(objects) = self.regions.get(&region_id) else {
            return;
        };
        let reached = (previous.map_or(Unbounded, Excluded), Included(entry_id));
        for (object_seq, _) in objects.range(reached).map(|(_, object)| object) {
            if let Some(live) = self.live_segments.get_mut(object_seq) {
                *live -= 1;
                if *live == 0 {
                    self.live_segments.remove(object_seq);
                }
            }
        }
    }

    /// Returns whether a segment is live: its region is not
    /// [`RELEASE_REGION_ID`], whose records recovery reads only from the
    /// objects it indexes, and either has no watermark or a watermark below
    /// the largest entry id of the segment. A region whose watermark is
    /// [`EntryId::MAX`] holds no live segment.
    fn is_live(&self, entry: &FooterEntry) -> bool {
        entry.region_id != RELEASE_REGION_ID
            && self
                .watermarks
                .get(&entry.region_id)
                .is_none_or(|watermark| entry.max_entry_id > *watermark)
    }

    /// Returns the reclaim boundary the indexed objects and the obsolete
    /// watermarks allow: the first indexed object at or above `from` that
    /// holds a live segment, see [`is_live`](Self::is_live), or the sequence
    /// after the last indexed object when none does, and never less than
    /// `from`. The objects holding a live segment are tracked as objects are
    /// indexed and watermarks move, so the boundary is a lookup.
    pub(crate) fn reclaim_boundary(&self, from: u64) -> u64 {
        match self.live_segments.range(from..).next() {
            Some((&object_seq, _)) => object_seq,
            None => self
                .objects
                .last_key_value()
                .map_or(from, |(&object_seq, _)| {
                    object_seq.saturating_add(1).max(from)
                }),
        }
    }

    /// Returns the objects that hold entries of `region_id` overlapping
    /// `start_entry_id..=end_entry_id`, ordered by object sequence. The
    /// segments of a region are ordered by entry id, so the lookup seeks to
    /// the first segment that ends at or after `start_entry_id` and stops
    /// before the first that starts after `end_entry_id`, whatever the number
    /// of objects the region holds.
    pub(crate) fn objects_for_entry_range(
        &self,
        region_id: RegionId,
        start_entry_id: u64,
        end_entry_id: u64,
    ) -> Result<Vec<(u64, &FooterEntry)>> {
        ensure!(
            start_entry_id <= end_entry_id,
            InvalidWalEntryRangeSnafu {
                region_id,
                start_entry_id,
                end_entry_id,
            }
        );

        let Some(objects) = self.regions.get(&region_id) else {
            return Ok(Vec::new());
        };
        Ok(objects
            .range(start_entry_id..)
            .map(|(_, (object_seq, entry))| (*object_seq, entry))
            .take_while(|(_, entry)| entry.min_entry_id <= end_entry_id)
            .collect())
    }

    /// Returns the largest entry id ever indexed for `region_id`, which is
    /// kept after the object holding it was removed.
    pub(crate) fn region_max_entry_id(&self, region_id: RegionId) -> Option<u64> {
        self.max_entry_ids.get(&region_id).copied()
    }

    /// Returns the sequence to assign to the next object written after recovery.
    ///
    /// An empty catalog starts at zero, so the first object of a prefix always
    /// takes sequence zero. Otherwise the sequence continues after the largest
    /// indexed one, which recovery discovers regardless of insertion order, and
    /// is raised further when the largest entry id of a region lies at or above
    /// the ids that sequence would assign: ids assigned under the earlier
    /// contiguous scheme carry no object information, and every new id of a
    /// region must be greater than every id it already has. A sequence at or
    /// above [`OBJECT_SEQ_LIMIT`] does not fit an entry id and is rejected.
    pub(crate) fn next_object_seq(&self) -> Result<u64> {
        let after_last = match self.objects.last_key_value() {
            None => 0,
            Some((&last_object_seq, _)) => last_object_seq
                .checked_add(1)
                .context(WalObjectSequenceExhaustedSnafu { last_object_seq })?,
        };
        let floor = self
            .max_entry_ids
            .values()
            .copied()
            .map(sequence_floor)
            .max()
            .unwrap_or(0);
        let next_object_seq = after_last.max(floor);
        ensure!(
            next_object_seq < OBJECT_SEQ_LIMIT,
            WalObjectSequenceExhaustedSnafu {
                last_object_seq: next_object_seq - 1,
            }
        );
        Ok(next_object_seq)
    }

    /// Iterates over the indexed objects ordered by object sequence.
    pub(crate) fn objects_in_order(&self) -> impl Iterator<Item = (u64, &[FooterEntry])> + '_ {
        self.objects
            .iter()
            .map(|(&object_seq, footer)| (object_seq, footer.as_slice()))
    }
}

fn out_of_order_reason(
    region_id: RegionId,
    lower_object_seq: u64,
    lower_max_entry_id: u64,
    upper_object_seq: u64,
    upper_min_entry_id: u64,
) -> String {
    format!(
        "entry ranges of region {region_id} are not strictly increasing, object {lower_object_seq} ends at {lower_max_entry_id}, object {upper_object_seq} starts at {upper_min_entry_id}"
    )
}

#[cfg(test)]
mod tests {
    use rand::rngs::StdRng;
    use rand::seq::SliceRandom;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::error::Error;
    use crate::object_store_wal::batch::entry_id;

    #[test]
    fn test_catalog_indexes_objects_and_queries_ranges() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(2, 1);
        let mut catalog = ObjectCatalog::default();

        // Recovery may discover objects out of order.
        catalog
            .insert_object(
                2,
                vec![
                    footer_entry(region_one, 4, 6),
                    footer_entry(region_two, 8, 9),
                ],
            )
            .unwrap();
        catalog
            .insert_object(1, vec![footer_entry(region_one, 1, 3)])
            .unwrap();
        catalog
            .insert_object(4, vec![footer_entry(region_one, 10, 12)])
            .unwrap();

        assert_eq!(Some(12), catalog.region_max_entry_id(region_one));
        assert_eq!(Some(9), catalog.region_max_entry_id(region_two));
        assert_eq!(None, catalog.region_max_entry_id(RegionId::new(3, 1)));

        let objects = catalog.objects_for_entry_range(region_one, 3, 10).unwrap();
        assert_eq!(vec![1, 2, 4], object_seqs(&objects));
        assert_eq!(
            vec![1, 2, 4],
            catalog
                .objects_in_order()
                .map(|(object_seq, _)| object_seq)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_catalog_rejects_duplicate_object_sequences() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        let first = footer_entry(region_id, 1, 2);
        let second = footer_entry(RegionId::new(2, 1), 4, 5);

        catalog
            .insert_object(1, vec![first.clone(), second.clone()])
            .unwrap();

        // An identical footer is rejected just like a conflicting one.
        assert_corrupted(
            catalog.insert_object(1, vec![second, first.clone()]),
            "object 1 is already indexed",
        );

        let mut conflicting = first;
        conflicting.segment_offset += 1;
        assert_corrupted(
            catalog.insert_object(1, vec![conflicting]),
            "object 1 is already indexed",
        );
        assert_eq!(1, catalog.objects_in_order().count());
    }

    #[test]
    fn test_catalog_resumes_object_sequence_after_recovery() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        assert_eq!(0, catalog.next_object_seq().unwrap());

        // Recovery may discover objects out of order.
        catalog
            .insert_object(4, vec![footer_entry(region_id, 10, 12)])
            .unwrap();
        catalog
            .insert_object(1, vec![footer_entry(region_id, 1, 3)])
            .unwrap();

        assert_eq!(5, catalog.next_object_seq().unwrap());

        // An object without segments takes its sequence all the same.
        catalog.insert_object(7, Vec::new()).unwrap();
        assert_eq!(8, catalog.next_object_seq().unwrap());
        assert_eq!(Some(12), catalog.region_max_entry_id(region_id));
    }

    #[test]
    fn test_catalog_raises_object_sequence_above_existing_entry_ids() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(1, 2);
        let mut catalog = ObjectCatalog::default();

        // Ids that fit below the ids of the next sequence leave it alone.
        catalog
            .insert_object(0, vec![footer_entry(region_one, 1, 3)])
            .unwrap();
        catalog
            .insert_object(
                1,
                vec![footer_entry(region_two, entry_id(1, 1), entry_id(1, 2))],
            )
            .unwrap();
        assert_eq!(2, catalog.next_object_seq().unwrap());

        // A contiguous id past them names a later object: the sequence
        // resumes above it, whichever region holds it.
        catalog
            .insert_object(2, vec![footer_entry(region_one, 5_000_000, 5_000_000)])
            .unwrap();
        assert_eq!(5, catalog.next_object_seq().unwrap());
        catalog
            .insert_object(
                3,
                vec![footer_entry(region_two, entry_id(7, 4), entry_id(7, 4))],
            )
            .unwrap();
        assert_eq!(8, catalog.next_object_seq().unwrap());
    }

    #[test]
    fn test_catalog_rejects_exhausted_object_sequence() {
        let region_id = RegionId::new(1, 1);
        let assert_exhausted = |catalog: &ObjectCatalog| {
            let error = catalog.next_object_seq().unwrap_err();
            assert!(
                error.to_string().contains("object sequence is exhausted"),
                "unexpected error: {error}"
            );
        };

        // The last sequence that fits an entry id is indexed.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(OBJECT_SEQ_LIMIT - 2, vec![footer_entry(region_id, 1, 2)])
            .unwrap();
        assert_eq!(OBJECT_SEQ_LIMIT - 1, catalog.next_object_seq().unwrap());
        catalog
            .insert_object(OBJECT_SEQ_LIMIT - 1, vec![footer_entry(region_id, 3, 4)])
            .unwrap();
        assert_exhausted(&catalog);

        // A sequence that does not fit was written by an earlier scheme.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(u64::MAX, vec![footer_entry(region_id, 1, 2)])
            .unwrap();
        assert_exhausted(&catalog);

        // An entry id that leaves no sequence above it.
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(0, vec![footer_entry(region_id, u64::MAX, u64::MAX)])
            .unwrap();
        assert_exhausted(&catalog);
    }

    #[test]
    fn test_catalog_rejects_overlapping_or_reversed_region_ranges() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        catalog
            .insert_object(2, vec![footer_entry(region_id, 10, 20)])
            .unwrap();

        assert_corrupted(
            catalog.insert_object(3, vec![footer_entry(region_id, 20, 30)]),
            "are not strictly increasing",
        );
        assert_corrupted(
            catalog.insert_object(3, vec![footer_entry(region_id, 5, 9)]),
            "are not strictly increasing",
        );
        assert_corrupted(
            catalog.insert_object(1, vec![footer_entry(region_id, 15, 19)]),
            "are not strictly increasing",
        );
        assert_eq!(1, catalog.objects_in_order().count());

        // A segment that fits between two others by its entry ids must also
        // fit between them by its sequence.
        catalog
            .insert_object(8, vec![footer_entry(region_id, 50, 60)])
            .unwrap();
        assert_corrupted(
            catalog.insert_object(1, vec![footer_entry(region_id, 30, 40)]),
            "object 1 ends at 40, object 2 starts at 10",
        );
        assert_corrupted(
            catalog.insert_object(9, vec![footer_entry(region_id, 30, 40)]),
            "object 8 ends at 60, object 9 starts at 30",
        );
        assert_corrupted(
            catalog.insert_object(5, vec![footer_entry(region_id, 61, 70)]),
            "object 5 ends at 70, object 8 starts at 50",
        );
        catalog
            .insert_object(5, vec![footer_entry(region_id, 30, 40)])
            .unwrap();
        assert_eq!(
            vec![2, 5, 8],
            object_seqs(
                &catalog
                    .objects_for_entry_range(region_id, 0, EntryId::MAX)
                    .unwrap()
            )
        );
    }

    #[test]
    fn test_catalog_rejects_duplicate_region_and_invalid_ranges() {
        let region_id = RegionId::new(1, 1);
        let mut catalog = ObjectCatalog::default();
        assert_corrupted(
            catalog.insert_object(
                1,
                vec![footer_entry(region_id, 1, 1), footer_entry(region_id, 2, 2)],
            ),
            "duplicate footer entries",
        );
        let mut invalid = footer_entry(region_id, 2, 1);
        invalid.entry_count = 0;
        assert_corrupted(
            catalog.insert_object(1, vec![invalid]),
            "has invalid entry range 2..=1",
        );

        let error = catalog
            .objects_for_entry_range(region_id, 2, 1)
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidWalEntryRange { start_entry_id, end_entry_id, .. } if start_entry_id == 2 && end_entry_id == 1),
            "unexpected error: {error:?}"
        );
    }

    #[test]
    fn test_catalog_reclaim_boundary_follows_the_watermarks() {
        let region_one = RegionId::new(1, 1);
        let region_two = RegionId::new(2, 1);
        let region_three = RegionId::new(3, 1);
        let mut catalog = ObjectCatalog::default();
        catalog.insert_object(0, Vec::new()).unwrap();
        catalog
            .insert_object(
                1,
                vec![
                    footer_entry(region_one, entry_id(1, 1), entry_id(1, 2)),
                    footer_entry(region_two, entry_id(1, 1), entry_id(1, 1)),
                ],
            )
            .unwrap();
        catalog
            .insert_object(
                2,
                vec![
                    footer_entry(region_one, entry_id(2, 1), entry_id(2, 1)),
                    footer_entry(RELEASE_REGION_ID, entry_id(2, 1), entry_id(2, 1)),
                ],
            )
            .unwrap();
        catalog
            .insert_object(
                3,
                vec![footer_entry(region_two, entry_id(3, 1), entry_id(3, 1))],
            )
            .unwrap();

        // A region without a watermark holds the boundary at its first object.
        assert_eq!(1, catalog.reclaim_boundary(0));
        catalog.obsolete(region_two, entry_id(1, 1));
        assert_eq!(1, catalog.reclaim_boundary(0));
        // A watermark inside a segment keeps its object.
        catalog.obsolete(region_one, entry_id(1, 1));
        assert_eq!(1, catalog.reclaim_boundary(0));
        catalog.obsolete(region_one, entry_id(1, 2));
        assert_eq!(2, catalog.reclaim_boundary(0));
        // The segment of release records never holds the boundary.
        catalog.obsolete(region_one, entry_id(2, 1));
        assert_eq!(3, catalog.reclaim_boundary(0));
        // A region whose watermark is `EntryId::MAX` holds nothing, and when
        // no region does, the boundary is past the last object.
        catalog.obsolete(region_two, EntryId::MAX);
        assert_eq!(4, catalog.reclaim_boundary(0));
        // A watermark never moves down, and the boundary is never below
        // `from`.
        catalog.obsolete(region_one, entry_id(1, 1));
        assert_eq!(4, catalog.reclaim_boundary(0));
        assert_eq!(5, catalog.reclaim_boundary(5));

        // A segment indexed at or below the watermark of its region is not
        // live, one of a region without a watermark is.
        catalog
            .insert_object(
                4,
                vec![footer_entry(region_two, entry_id(4, 1), entry_id(4, 1))],
            )
            .unwrap();
        assert_eq!(5, catalog.reclaim_boundary(0));
        catalog
            .insert_object(
                5,
                vec![footer_entry(region_three, entry_id(5, 1), entry_id(5, 1))],
            )
            .unwrap();
        assert_eq!(5, catalog.reclaim_boundary(0));
        catalog.obsolete(region_three, entry_id(5, 1));
        assert_eq!(6, catalog.reclaim_boundary(0));

        // Removing objects keeps the largest entry id of every region, and a
        // region without indexed objects contributes nothing.
        for object_seq in [0, 1, 2] {
            assert!(catalog.remove_object(object_seq).is_some());
        }
        assert!(catalog.remove_object(2).is_none());
        assert!(!catalog.contains_object(2));
        assert!(catalog.contains_object(3));
        assert_eq!(
            Some(entry_id(2, 1)),
            catalog.region_max_entry_id(region_one)
        );
        assert!(
            catalog
                .objects_for_entry_range(region_one, 0, EntryId::MAX)
                .unwrap()
                .is_empty()
        );
        assert_eq!(6, catalog.reclaim_boundary(0));
        assert_eq!(6, catalog.next_object_seq().unwrap());
    }

    /// Random appends, watermark moves, releases, drops, collections and
    /// recoveries give the reclaim boundary that a scan of the footers from
    /// `from` gives, whatever `from` is, and the objects of a region that a
    /// scan finds for a range of entry ids.
    #[test]
    fn test_catalog_matches_a_scan_of_the_footers() {
        let regions = (1..=6)
            .map(|number| RegionId::new(1, number))
            .collect::<Vec<_>>();
        for seed in 0..256 {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut catalog = ObjectCatalog::default();
            let mut watermarks = HashMap::new();
            // The largest entry id written per region, which a later segment
            // of the region is above.
            let mut written = HashMap::new();
            let mut next_object_seq = 0;
            let mut boundary = 0;
            for step in 0..128 {
                let mut releases = rng.random_bool(0.2);
                match rng.random_range(0..8) {
                    0..=2 => {}
                    // A watermark move, which may be below the watermark or
                    // above every id written, of a region or, rarely, of the
                    // reserved region, which holds nothing live either way.
                    3 | 4 => {
                        let region_id = if rng.random_bool(0.1) {
                            RELEASE_REGION_ID
                        } else {
                            regions[rng.random_range(0..regions.len())]
                        };
                        let ceiling = written.get(&region_id).copied().unwrap_or(0) + 2;
                        let entry_id = rng.random_range(0..=ceiling);
                        obsolete(&mut catalog, &mut watermarks, region_id, entry_id);
                    }
                    // A release through the largest id written, or a drop,
                    // seals an object with a release record.
                    5 => {
                        let region_id = regions[rng.random_range(0..regions.len())];
                        let entry_id = if rng.random_bool(0.5) {
                            written.get(&region_id).copied().unwrap_or(0)
                        } else {
                            EntryId::MAX
                        };
                        obsolete(&mut catalog, &mut watermarks, region_id, entry_id);
                        releases = true;
                    }
                    // A collection deletes objects below the boundary, in any
                    // order and not always all of them. Removing an object
                    // above it, which may hold live segments, is followed too.
                    6 => {
                        let collected = catalog
                            .objects_in_order()
                            .map(|(object_seq, _)| object_seq)
                            .filter(|object_seq| {
                                (*object_seq < boundary || rng.random_bool(0.1))
                                    && rng.random_bool(0.7)
                            })
                            .collect::<Vec<_>>();
                        for object_seq in collected {
                            assert!(catalog.remove_object(object_seq).is_some());
                        }
                    }
                    // A recovery indexes the objects in any order, then the
                    // watermarks.
                    _ => {
                        let mut objects = catalog
                            .objects_in_order()
                            .map(|(object_seq, footer)| (object_seq, footer.to_vec()))
                            .collect::<Vec<_>>();
                        objects.shuffle(&mut rng);
                        catalog = ObjectCatalog::default();
                        for (object_seq, footer) in objects {
                            catalog.insert_object(object_seq, footer).unwrap();
                        }
                        for (region_id, entry_id) in &watermarks {
                            catalog.obsolete(*region_id, *entry_id);
                        }
                    }
                }
                if rng.random_bool(0.5) || releases {
                    let segments = regions
                        .iter()
                        .copied()
                        .filter(|_| rng.random_bool(0.5))
                        .chain(releases.then_some(RELEASE_REGION_ID))
                        .collect::<Vec<_>>();
                    let mut footer = Vec::new();
                    for region_id in segments {
                        let min_entry_id = written.get(&region_id).copied().unwrap_or(0)
                            + 1
                            + rng.random_range(0..3);
                        let max_entry_id = min_entry_id + rng.random_range(0..3);
                        written.insert(region_id, max_entry_id);
                        footer.push(footer_entry(region_id, min_entry_id, max_entry_id));
                    }
                    catalog.insert_object(next_object_seq, footer).unwrap();
                    next_object_seq += rng.random_range(1..=2);
                }

                let expected = scanned_reclaim_boundary(&catalog, boundary, &watermarks);
                assert_eq!(
                    expected,
                    catalog.reclaim_boundary(boundary),
                    "seed {seed}, step {step}"
                );
                let from = rng.random_range(0..=next_object_seq + 1);
                assert_eq!(
                    scanned_reclaim_boundary(&catalog, from, &watermarks),
                    catalog.reclaim_boundary(from),
                    "seed {seed}, step {step}, from {from}"
                );
                boundary = expected;

                for region_id in regions.iter().chain([&RELEASE_REGION_ID]) {
                    let ceiling = written.get(region_id).copied().unwrap_or(0) + 2;
                    let start = rng.random_range(0..=ceiling);
                    let end = rng.random_range(start..=ceiling);
                    let scanned = catalog
                        .objects_in_order()
                        .filter(|(_, footer)| {
                            footer.iter().any(|entry| {
                                entry.region_id == *region_id
                                    && entry.max_entry_id >= start
                                    && entry.min_entry_id <= end
                            })
                        })
                        .map(|(object_seq, _)| object_seq)
                        .collect::<Vec<_>>();
                    assert_eq!(
                        scanned,
                        object_seqs(
                            &catalog
                                .objects_for_entry_range(*region_id, start, end)
                                .unwrap()
                        ),
                        "seed {seed}, step {step}, region {region_id}, {start}..={end}"
                    );
                }
            }
        }
    }

    fn obsolete(
        catalog: &mut ObjectCatalog,
        watermarks: &mut HashMap<RegionId, EntryId>,
        region_id: RegionId,
        entry_id: EntryId,
    ) {
        catalog.obsolete(region_id, entry_id);
        watermarks
            .entry(region_id)
            .and_modify(|watermark: &mut EntryId| *watermark = (*watermark).max(entry_id))
            .or_insert(entry_id);
    }

    /// The first object at or above `from` holding a segment that is live
    /// under `watermarks`, found by reading every footer, or the sequence
    /// after the last object when none does, and never less than `from`.
    fn scanned_reclaim_boundary(
        catalog: &ObjectCatalog,
        from: u64,
        watermarks: &HashMap<RegionId, EntryId>,
    ) -> u64 {
        let live = catalog
            .objects_in_order()
            .filter(|(object_seq, _)| *object_seq >= from)
            .find(|(_, footer)| {
                footer.iter().any(|entry| {
                    entry.region_id != RELEASE_REGION_ID
                        && watermarks
                            .get(&entry.region_id)
                            .is_none_or(|watermark| entry.max_entry_id > *watermark)
                })
            });
        match live {
            Some((object_seq, _)) => object_seq,
            None => catalog
                .objects_in_order()
                .last()
                .map_or(from, |(object_seq, _)| {
                    object_seq.saturating_add(1).max(from)
                }),
        }
    }

    fn footer_entry(region_id: RegionId, min_entry_id: u64, max_entry_id: u64) -> FooterEntry {
        FooterEntry {
            region_id,
            min_entry_id,
            max_entry_id,
            entry_count: max_entry_id
                .checked_sub(min_entry_id)
                .and_then(|count| count.checked_add(1))
                .unwrap_or(0) as u32,
            segment_offset: min_entry_id.wrapping_mul(100),
            segment_len: 100,
            segment_crc32: min_entry_id as u32,
        }
    }

    fn object_seqs(objects: &[(u64, &FooterEntry)]) -> Vec<u64> {
        objects.iter().map(|(object_seq, _)| *object_seq).collect()
    }

    fn assert_corrupted(result: Result<()>, expected_reason: &str) {
        match result {
            Err(Error::CorruptedWalObject { reason, .. }) => assert!(
                reason.contains(expected_reason),
                "expected reason to contain {expected_reason:?}, actual {reason:?}"
            ),
            other => panic!("expected a corrupted object error, actual {other:?}"),
        }
    }
}
