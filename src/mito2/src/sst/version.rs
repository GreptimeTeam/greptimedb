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

//! SST version.
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use common_time::{TimeToLive, Timestamp};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{FileId, RegionId};

use crate::sst::file::{self, FileHandle, FileMeta, FileMetaRef, FileTimeRange, Level};
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::primary_key::PrimaryKeyRangeMapper;

/// A version of all SSTs in a region.
#[derive(Debug, Clone)]
pub(crate) struct SstVersion {
    /// SST metadata organized by levels.
    levels: Arc<LevelMetaArray>,
    series_indexes: Arc<HashMap<FileId, FileHandle>>,
    primary_key_mapper: Arc<PrimaryKeyRangeMapper>,
}

pub(crate) type SstVersionRef = Arc<SstVersion>;

impl SstVersion {
    /// Returns a new [SstVersion].
    pub(crate) fn new(metadata: RegionMetadataRef) -> SstVersion {
        SstVersion {
            levels: Arc::new(new_level_meta_vec()),
            series_indexes: Arc::new(HashMap::new()),
            primary_key_mapper: Arc::new(PrimaryKeyRangeMapper::new(metadata)),
        }
    }

    pub(crate) fn series_indexes(&self) -> &HashMap<FileId, FileHandle> {
        &self.series_indexes
    }

    pub(crate) fn add_series_indexes(
        &mut self,
        purger: FilePurgerRef,
        files: impl Iterator<Item = impl Into<FileMetaRef>>,
    ) {
        for file in files {
            let indexes = Arc::make_mut(&mut self.series_indexes);
            let file = file.into();
            indexes
                .entry(file.file_id)
                .or_insert_with(|| FileHandle::new(file, purger.clone()));
        }
    }

    pub(crate) fn remove_series_indexes(&mut self, ids: impl Iterator<Item = file::RegionFileId>) {
        for id in ids {
            let indexes = Arc::make_mut(&mut self.series_indexes);
            if indexes
                .get(&id.file_id())
                .is_some_and(|file| file.file_id() == id)
                && let Some(file) = indexes.remove(&id.file_id())
            {
                file.mark_deleted();
            }
        }
    }

    /// Changes the target schema without copying the SST list or rebinding handles.
    pub(crate) fn set_metadata(&mut self, metadata: RegionMetadataRef) {
        self.primary_key_mapper = Arc::new(self.primary_key_mapper.with_metadata(metadata));
    }

    /// Shares the target schema and encoded defaults with comparisons.
    pub(crate) fn primary_key_mapper(&self) -> Arc<PrimaryKeyRangeMapper> {
        self.primary_key_mapper.clone()
    }

    /// Returns a slice to metadatas of all levels.
    pub(crate) fn levels(&self) -> &[LevelMeta] {
        self.levels.as_ref()
    }

    /// Returns the current handle matching the selected file's identity in its immutable level.
    pub(crate) fn file_for_compaction(&self, selected: &FileHandle) -> Option<&FileHandle> {
        let current = self
            .levels
            .get(selected.level() as usize)?
            .files
            .get(&selected.file_id().file_id())?;
        (current.file_id() == selected.file_id()).then_some(current)
    }

    /// Add files to the version. If a file with the same `file_id` already exists,
    /// it will be overwritten with the new file.
    ///
    /// # Panics
    /// Panics if the level of [FileMeta] has no storage slot.
    pub(crate) fn add_files(
        &mut self,
        file_purger: FilePurgerRef,
        files_to_add: impl Iterator<Item = impl Into<FileMetaRef>>,
    ) {
        let levels = Arc::make_mut(&mut self.levels);
        for file in files_to_add {
            let file = file.into();
            let level = file.level;
            // A trusted L0 adoption changes classification, not the physical object.
            // Detach the old handle without marking it deleted (old readers may hold it).
            // Admission rejects an existing L1 reference; see `prepare_region_edit`.
            // The detached description still takes part in the reconciliation below:
            // dropping it before reconciling would let an incoming description install
            // wholesale and lose an index view only the L0 entry knew, so a reopen that
            // still holds that entry would disagree with the live version.
            let detached = if level == file::LEVEL_SHARED {
                levels[0]
                    .files
                    .remove(&file.file_id)
                    .map(|handle| handle.meta_ref().clone())
            } else {
                None
            };
            // Shared references use a sentinel wire level and live in their own slot.
            let Some(slot) = file::level_slot(level) else {
                panic!("Unsupported SST level {level}");
            };
            // If the same physical SST is already visible, publish one reconciled
            // description of it. `reconcile_reference` is the same rule a reopen
            // applies, so a restart cannot change what this version publishes; the
            // incoming row authorization always wins because it belongs to one
            // reference, and a fresh handle would drop a running compaction's
            // reservation, which `with_updated_index` shares instead.
            match levels[slot].files.entry(file.file_id) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    let existing = entry.get().meta_ref().clone();
                    // Reuse only what is unchanged, and "unchanged" compares the row
                    // authorization by its serialized form: `PartitionExpr` equality
                    // cannot tell `TIME 1 second` from `TIME 1000 milliseconds` apart,
                    // so the derived equality would keep a range the staging proof
                    // already replaced.
                    if existing.same_reference(&file) && detached.is_none() {
                        continue;
                    }
                    if existing.exists_index()
                        && !existing.supersedes_index_view(&file)
                        && existing.index_version > file.index_version
                    {
                        // A description older than the current index view: only its
                        // row authorization is news.
                        common_telemetry::warn!(
                            "Adding file with older index version, existing: {:?}, new: {:?}, keeping the current index view",
                            existing,
                            file
                        );
                    }
                    let reconciled = Self::reconcile_descriptions(
                        [detached.as_ref(), Some(&existing)],
                        (*file).clone(),
                    );
                    if !reconciled.same_reference(&existing) {
                        *entry.get_mut() = entry
                            .get()
                            .with_updated_index(FileMetaRef::from(reconciled));
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    let reconciled =
                        Self::reconcile_descriptions([detached.as_ref()], (*file).clone());
                    entry.insert(FileHandle::new(
                        FileMetaRef::from(reconciled),
                        file_purger.clone(),
                    ));
                }
            }
        }
    }

    /// Reconciles an incoming description with every description of the same
    /// physical SST this version already holds.
    ///
    /// The row authorization belongs to `incoming`; the index view comes from
    /// whichever description is newest under [FileMeta::reconcile_reference], so
    /// a live update and a reopen publish the same thing.
    fn reconcile_descriptions<'a>(
        prior: impl IntoIterator<Item = Option<&'a FileMeta>>,
        incoming: FileMeta,
    ) -> FileMeta {
        let mut reconciled = incoming;
        for prior in prior.into_iter().flatten() {
            reconciled = prior.reconcile_reference(&reconciled);
        }
        reconciled
    }

    /// Remove files from the version.
    ///
    /// # Panics
    /// Panics if the level of [FileMeta] has no storage slot.
    pub(crate) fn remove_files(&mut self, files_to_remove: impl Iterator<Item = FileMeta>) {
        let levels = Arc::make_mut(&mut self.levels);
        for file in files_to_remove {
            let Some(slot) = file::level_slot(file.level) else {
                panic!("Unsupported SST level {}", file.level);
            };
            if let Some(handle) = levels[slot].files.remove(&file.file_id) {
                handle.mark_deleted();
            }
        }
    }

    /// Marks all SSTs in this version as deleted.
    pub(crate) fn mark_all_deleted(&self) {
        for file in self.series_indexes.values() {
            file.mark_deleted();
        }
        for level_meta in self.levels.iter() {
            for file_handle in level_meta.files.values() {
                file_handle.mark_deleted();
            }
        }
    }

    /// Returns the number of rows in SST files owned by `region_id`.
    ///
    /// Rows from SST files referenced from other regions, for example after
    /// repartition, are not counted.
    /// For historical reasons, the result is not precise for old SST files.
    pub(crate) fn owned_num_rows(&self, region_id: RegionId) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .filter(|file_handle| file_handle.region_id() == region_id)
                    .map(|file_handle| {
                        let meta = file_handle.meta_ref();
                        meta.num_rows
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    /// Returns the number of SST files owned by `region_id`.
    pub(crate) fn owned_num_files(&self, region_id: RegionId) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .filter(|file_handle| file_handle.region_id() == region_id)
                    .count() as u64
            })
            .sum()
    }

    /// Returns the time range covered by every file in this version, including
    /// files referenced from other regions after a repartition.
    pub(crate) fn time_range(&self) -> Option<FileTimeRange> {
        self.levels
            .iter()
            .flat_map(|level_meta| level_meta.files.values())
            .map(|file_handle| file_handle.time_range())
            .reduce(|(min_a, max_a), (min_b, max_b)| (min_a.min(min_b), max_a.max(max_b)))
    }

    /// Returns the space occupied by SST data files owned by `region_id`.
    pub(crate) fn owned_sst_usage(&self, region_id: RegionId) -> u64 {
        self.levels
            .iter()
            .map(|level_meta| {
                level_meta
                    .files
                    .values()
                    .filter(|file_handle| file_handle.region_id() == region_id)
                    .map(|file_handle| {
                        let meta = file_handle.meta_ref();
                        meta.file_size
                    })
                    .sum::<u64>()
            })
            .sum()
    }

    /// Returns the space occupied by SST index files owned by `region_id`.
    pub(crate) fn owned_index_usage(&self, region_id: RegionId) -> u64 {
        let parquet = self
            .levels
            .iter()
            .flat_map(|level| level.files.values())
            .filter(|handle| handle.region_id() == region_id)
            .map(|handle| handle.meta_ref().index_file_size);
        let series = self
            .series_indexes()
            .values()
            .filter(|handle| handle.region_id() == region_id)
            .map(|handle| handle.meta_ref().file_size);
        parquet.chain(series).sum()
    }
}

// We only has fixed number of level, so we use array to hold elements. This implementation
// detail of LevelMetaArray should not be exposed to users of [LevelMetas].
type LevelMetaArray = [LevelMeta; file::LEVEL_SLOT_COUNT];

/// Metadata of files in the same SST level.
#[derive(Clone)]
pub struct LevelMeta {
    /// Level number.
    pub level: Level,
    /// Handles of SSTs in this level.
    pub files: HashMap<FileId, FileHandle>,
}

impl LevelMeta {
    /// Returns an empty meta of specific `level`.
    pub(crate) fn new(level: Level) -> LevelMeta {
        LevelMeta {
            level,
            files: HashMap::new(),
        }
    }

    /// Returns expired SSTs from current level.
    pub fn get_expired_files(&self, now: &Timestamp, ttl: &TimeToLive) -> Vec<FileHandle> {
        self.files
            .values()
            .filter(|v| {
                let (_, end) = v.time_range();

                match ttl.is_expired(&end, now) {
                    Ok(expired) => expired,
                    Err(e) => {
                        common_telemetry::error!(e; "Failed to calculate region TTL expire time");
                        false
                    }
                }
            })
            .cloned()
            .collect()
    }

    pub fn files(&self) -> impl Iterator<Item = &FileHandle> {
        self.files.values()
    }
}

impl fmt::Debug for LevelMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LevelMeta")
            .field("level", &self.level)
            .field("files", &self.files.keys())
            .finish()
    }
}

fn new_level_meta_vec() -> LevelMetaArray {
    // Slot 2 keeps the sentinel wire level so level-based filters still see shared files.
    [
        LevelMeta::new(0),
        LevelMeta::new(1),
        LevelMeta::new(file::LEVEL_SHARED),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::new_noop_file_purger;

    #[test]
    fn time_range_spans_files_referenced_from_other_regions() {
        let purger = new_noop_file_purger();
        let owned = FileMeta {
            file_id: FileId::random(),
            region_id: RegionId::new(1, 1),
            time_range: (
                Timestamp::new_millisecond(200),
                Timestamp::new_millisecond(300),
            ),
            ..Default::default()
        };
        let referenced = FileMeta {
            file_id: FileId::random(),
            region_id: RegionId::new(2, 1),
            time_range: (
                Timestamp::new_millisecond(50),
                Timestamp::new_millisecond(100),
            ),
            ..Default::default()
        };

        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(purger, [owned, referenced].into_iter());

        assert_eq!(
            version.time_range(),
            Some((
                Timestamp::new_millisecond(50),
                Timestamp::new_millisecond(300)
            ))
        );
    }

    #[test]
    fn time_range_compares_across_units() {
        let purger = new_noop_file_purger();
        let seconds = FileMeta {
            file_id: FileId::random(),
            time_range: (Timestamp::new_second(1), Timestamp::new_second(2)),
            ..Default::default()
        };
        let millis = FileMeta {
            file_id: FileId::random(),
            time_range: (
                Timestamp::new_millisecond(500),
                Timestamp::new_millisecond(2500),
            ),
            ..Default::default()
        };

        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(purger, [seconds, millis].into_iter());

        assert_eq!(
            version.time_range(),
            Some((
                Timestamp::new_millisecond(500),
                Timestamp::new_millisecond(2500)
            ))
        );
    }

    #[test]
    fn time_range_is_none_without_files() {
        let version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        assert_eq!(version.time_range(), None);
    }

    #[rstest::rstest]
    fn test_index_replacement_shares_retirement_with_old_readers(
        #[values(false, true)] retire_before_update: bool,
    ) {
        let purger = new_noop_file_purger();
        let meta = FileMeta {
            file_id: FileId::random(),
            ..Default::default()
        };
        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(purger.clone(), [meta.clone()].into_iter());
        let old_snapshot = version.clone();
        let old_reader = old_snapshot.levels()[0].files[&meta.file_id].clone();
        let independent = FileHandle::new(meta.clone(), purger.clone());
        if retire_before_update {
            old_reader.mark_deleted();
        }
        let mut updated = meta.clone();
        updated.index_version = 1;
        updated
            .available_indexes
            .push(crate::sst::file::IndexType::InvertedIndex);
        version.add_files(purger, [updated.clone()].into_iter());
        let new_reader = version.levels()[0].files[&meta.file_id].clone();

        assert_eq!(old_reader.meta_ref(), &meta);
        assert_eq!(new_reader.meta_ref(), &updated);
        assert_eq!(new_reader.is_deleted(), retire_before_update);
        version.remove_files([updated].into_iter());
        assert!(new_reader.is_deleted());
        assert!(old_reader.is_deleted());
        assert!(!independent.is_deleted());
        drop(version);
        assert!(old_snapshot.levels()[0].files[&meta.file_id].is_deleted());
    }

    /// Mirrors the reservation guard in `compaction::scheduler::state`:
    /// it releases whatever handle it was created from.
    struct ReservationGuard(FileHandle);

    impl Drop for ReservationGuard {
        fn drop(&mut self) {
            self.0.set_compacting(false);
        }
    }

    #[test]
    fn test_range_replacement_releases_the_compaction_reservation() {
        let purger = new_noop_file_purger();
        let meta = FileMeta {
            file_id: FileId::random(),
            ..Default::default()
        };
        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(purger.clone(), [meta.clone()].into_iter());

        // A compaction plan reserves the handle it selected.
        let selected = version.levels()[0].files[&meta.file_id].clone();
        assert!(selected.try_set_compacting());
        let guard = ReservationGuard(selected.clone());

        // Repartition replaces the handle with a range-only view of the same SST.
        let mut ranged = meta.clone();
        ranged.owned_range = Some(crate::sst::file::PartitionExprRef::from(
            partition::expr::col("tag").lt(datatypes::value::Value::UInt32(10)),
        ));
        version.add_files(purger, [ranged.clone()].into_iter());
        let live = version.levels()[0].files[&meta.file_id].clone();
        assert_eq!(live.meta_ref(), &ranged);
        assert!(
            live.compacting(),
            "the replacement must inherit the running reservation"
        );

        // Cancelling the compaction must release the handle the version publishes.
        drop(guard);
        assert!(!live.compacting(), "the reservation must not leak");
        assert!(
            live.try_set_compacting(),
            "a later plan must be able to reserve the file again"
        );
    }

    #[test]
    fn test_add_files() {
        let purger = new_noop_file_purger();

        let files = (1..=3)
            .map(|_| FileMeta {
                file_id: FileId::random(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        // files[1] is added multiple times, and that's ok.
        version.add_files(purger.clone(), files[..=1].iter().cloned());
        version.add_files(purger, files[1..].iter().cloned());

        let added_files = &version.levels()[0].files;
        assert_eq!(added_files.len(), 3);
        files.iter().for_each(|f| {
            assert!(added_files.contains_key(&f.file_id));
        });
    }

    #[test]
    fn test_file_for_compaction_uses_selected_level() {
        let purger = new_noop_file_purger();
        let file_id = FileId::random();
        let selected = FileHandle::new(
            FileMeta {
                file_id,
                level: 1,
                ..Default::default()
            },
            purger.clone(),
        );
        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(
            purger,
            [
                FileMeta {
                    file_id,
                    level: 0,
                    ..Default::default()
                },
                selected.meta_ref().clone(),
            ]
            .into_iter(),
        );

        let current = version.file_for_compaction(&selected).unwrap();
        assert_eq!(selected.file_id(), current.file_id());
        assert_eq!(selected.level(), current.level());
    }

    #[test]
    fn test_usage_only_counts_owned_files() {
        let purger = new_noop_file_purger();
        let region_id = RegionId::new(1, 1);
        let other_region_id = RegionId::new(1, 2);

        let files = [
            FileMeta {
                region_id,
                file_id: FileId::random(),
                file_size: 100,
                index_file_size: 10,
                num_rows: 1,
                ..Default::default()
            },
            FileMeta {
                region_id,
                file_id: FileId::random(),
                file_size: 200,
                index_file_size: 20,
                num_rows: 2,
                ..Default::default()
            },
            FileMeta {
                region_id: other_region_id,
                file_id: FileId::random(),
                file_size: 300,
                index_file_size: 30,
                num_rows: 3,
                ..Default::default()
            },
        ];

        let mut version = SstVersion::new(crate::test_util::memtable_util::metadata_for_test());
        version.add_files(purger, files.iter().cloned());

        assert_eq!(3, version.owned_num_rows(region_id));
        assert_eq!(2, version.owned_num_files(region_id));
        assert_eq!(300, version.owned_sst_usage(region_id));
        assert_eq!(30, version.owned_index_usage(region_id));
        assert_eq!(3, version.owned_num_rows(other_region_id));
        assert_eq!(1, version.owned_num_files(other_region_id));
        assert_eq!(300, version.owned_sst_usage(other_region_id));
        assert_eq!(30, version.owned_index_usage(other_region_id));
    }
}
