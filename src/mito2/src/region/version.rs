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

//! Version control of mito engine.
//!
//! Version is an immutable snapshot of region's metadata.
//!
//! To read latest data from `VersionControl`, we should
//! 1. Acquire `Version` from `VersionControl`.
//! 2. Then acquire last sequence.
//!
//! Reason: data may be flushed/compacted and some data with old sequence may be removed
//! and became invisible between step 1 and 2, so need to acquire version at first.

use std::sync::{Arc, RwLock};
use std::time::Duration;

use common_telemetry::info;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{RegionId, SequenceNumber};

use crate::error::{RegionPendingTailsSnafu, Result};
use crate::manifest::action::{PendingTail, RegionEdit, TruncateKind};
use crate::memtable::time_partition::TimePartitions;
use crate::memtable::version::{MemtableVersion, MemtableVersionRef};
use crate::memtable::{MemtableBuilderRef, MemtableId};
use crate::region::options::RegionOptions;
use crate::sst::file::FileMeta;
use crate::sst::file_purger::FilePurgerRef;
use crate::sst::version::{SstVersion, SstVersionRef};
use crate::wal::EntryId;

/// Controls metadata and sequence numbers for a region.
///
/// It manages metadata in a copy-on-write fashion. Any modification to a region's metadata
/// will generate a new [Version].
#[derive(Debug)]
pub(crate) struct VersionControl {
    data: RwLock<VersionControlData>,
}

impl VersionControl {
    /// Returns a new [VersionControl] with specific `version`.
    pub(crate) fn new(version: Version) -> VersionControl {
        // Initialize sequence and entry id from flushed sequence and entry id.
        let (flushed_sequence, flushed_entry_id) =
            (version.flushed_sequence, version.flushed_entry_id);
        VersionControl {
            data: RwLock::new(VersionControlData {
                version: Arc::new(version),
                last_sequence: flushed_sequence,
                committed_sequence: flushed_sequence,
                last_entry_id: flushed_entry_id,
                is_dropped: false,
            }),
        }
    }

    /// Returns the id of the region controlled by this instance.
    pub(crate) fn region_id(&self) -> RegionId {
        self.data.read().unwrap().version.metadata.region_id
    }

    /// Returns current copy of data.
    pub(crate) fn current(&self) -> VersionControlData {
        self.data.read().unwrap().clone()
    }

    /// Updates last sequence number to the maximum.
    pub(crate) fn update_last_sequence(&self, seq: SequenceNumber) {
        let mut data = self.data.write().unwrap();
        data.last_sequence = data.last_sequence.max(seq);
    }

    /// Updates the `committed_sequence` of version.
    pub(crate) fn set_committed_sequence(&self, seq: SequenceNumber) {
        let mut data = self.data.write().unwrap();
        data.committed_sequence = data.committed_sequence.max(seq);
        data.last_sequence = data.last_sequence.max(data.committed_sequence);
    }

    /// Updates committed sequence and entry id.
    pub(crate) fn set_sequence_and_entry_id(&self, seq: SequenceNumber, entry_id: EntryId) {
        let mut data = self.data.write().unwrap();
        data.committed_sequence = data.committed_sequence.max(seq);
        data.last_sequence = data.last_sequence.max(data.committed_sequence);
        data.last_entry_id = entry_id;
    }

    /// Updates last entry id.
    pub(crate) fn set_entry_id(&self, entry_id: EntryId) {
        let mut data = self.data.write().unwrap();
        data.last_entry_id = entry_id;
    }

    /// Sequence number of last committed data.
    pub(crate) fn committed_sequence(&self) -> SequenceNumber {
        self.data.read().unwrap().committed_sequence
    }

    /// Freezes the mutable memtable if it is not empty.
    pub(crate) fn freeze_mutable(&self) -> Result<()> {
        let version = self.current().version;
        let time_window = version.compaction_time_window;

        let Some(new_memtables) = version
            .memtables
            .freeze_mutable(&version.metadata, time_window)?
        else {
            return Ok(());
        };

        // Create a new version with memtable switched.
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .memtables(new_memtables)
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;

        Ok(())
    }

    /// Applies region option changes and generates a new version.
    pub(crate) fn alter_options(&self, options: RegionOptions) {
        let version = self.current().version;
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .options(options)
                .build(),
        );
        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Apply edit to current version.
    ///
    /// If `edit` is None, only removes the specified memtables.
    pub(crate) fn apply_edit(
        &self,
        edit: Option<RegionEdit>,
        memtables_to_remove: &[MemtableId],
        purger: FilePurgerRef,
    ) {
        self.apply_edit_with_metadata(None, edit, memtables_to_remove, purger);
    }

    /// Applies an optional metadata update and an optional edit as **one**
    /// [Version] publish.
    ///
    /// Publishing both together keeps a concurrent reader from observing the
    /// new metadata (e.g. a changed partition expression) together with the
    /// previous file set or sequence boundary, which would break the invariant
    /// that one scan sees one consistent file set and partition rule.
    pub(crate) fn apply_edit_with_metadata(
        &self,
        metadata: Option<RegionMetadataRef>,
        edit: Option<RegionEdit>,
        memtables_to_remove: &[MemtableId],
        purger: FilePurgerRef,
    ) {
        let version = self.current().version;
        let builder = VersionBuilder::from_version(version);
        let builder = if let Some(metadata) = metadata {
            builder.metadata(metadata)
        } else {
            builder
        };
        let committed_sequence = edit.as_ref().and_then(|e| e.committed_sequence);
        let builder = if let Some(edit) = edit {
            builder.apply_edit(edit, purger)
        } else {
            builder
        };
        let new_version = Arc::new(builder.remove_memtables(memtables_to_remove).build());

        let mut version_data = self.data.write().unwrap();
        version_data.committed_sequence = if let Some(committed_in_edit) = committed_sequence {
            version_data.committed_sequence.max(committed_in_edit)
        } else {
            version_data.committed_sequence
        };
        version_data.last_sequence = version_data
            .last_sequence
            .max(version_data.committed_sequence);
        version_data.version = new_version;
    }

    /// Mark all opened files as deleted and set the delete marker in [VersionControlData]
    pub(crate) fn mark_dropped(&self) {
        let version = self.current().version;
        let memtable_builder = version.memtables.mutable.memtable_builder().clone();
        let new_mutable =
            Self::new_mutable_from_version(&version, version.metadata.clone(), memtable_builder);

        let mut data = self.data.write().unwrap();
        data.is_dropped = true;
        data.version.ssts.mark_all_deleted();
        // Reset version so we can release the reference to memtables and SSTs.
        let new_version =
            Arc::new(VersionBuilder::new(version.metadata.clone(), new_mutable).build());
        data.version = new_version;
    }

    /// Alter schema of the region.
    ///
    /// It replaces existing mutable memtable with a memtable that uses the
    /// new schema. Memtables of the version must be empty.
    pub(crate) fn alter_schema(&self, metadata: RegionMetadataRef) {
        let version = self.current().version;
        let memtable_builder = version.memtables.mutable.memtable_builder().clone();
        let new_mutable =
            Self::new_mutable_from_version(&version, metadata.clone(), memtable_builder);
        debug_assert!(version.memtables.mutable.is_empty());
        debug_assert!(version.memtables.immutables().is_empty());
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .metadata(metadata)
                .memtables(MemtableVersion::new(new_mutable))
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Alter metadata of the region without rebuilding memtables.
    ///
    /// Production paths that must keep metadata and files consistent publish both
    /// through [Self::apply_edit_with_metadata]; this metadata-only helper is kept
    /// for tests that need an isolated metadata publish.
    #[cfg(test)]
    pub(crate) fn alter_metadata(&self, metadata: RegionMetadataRef) {
        let version = self.current().version;
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .metadata(metadata)
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Alter schema and format of the region.
    ///
    /// It replaces existing mutable memtable with a memtable that uses the
    /// new format. Memtables of the version must be empty.
    pub(crate) fn alter_schema_and_format(
        &self,
        metadata: RegionMetadataRef,
        options: RegionOptions,
        memtable_builder: MemtableBuilderRef,
    ) {
        let version = self.current().version;
        // Use the new metadata to build `TimePartitions`.
        let new_mutable =
            Self::new_mutable_from_version(&version, metadata.clone(), memtable_builder);
        debug_assert!(version.memtables.mutable.is_empty());
        debug_assert!(version.memtables.immutables().is_empty());
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .metadata(metadata)
                .options(options)
                .memtables(MemtableVersion::new(new_mutable))
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Truncate current version.
    ///
    /// Truncation drops rows from the region, which changes which rows it owns, so
    /// the truncate action carries the edit boundary allocated from
    /// `last_sequence + 1`. The manifest records the same value, so a reopened
    /// region keeps rejecting cursors that predate the truncation. When the action
    /// carries no boundary (an action written before the field existed) the
    /// boundary is allocated here.
    pub(crate) fn truncate(
        &self,
        truncate_kind: TruncateKind,
        last_invalid_seq: Option<SequenceNumber>,
    ) {
        let current = self.current();
        let version = current.version;
        // An action written before the field existed carries no boundary: derive
        // one through the shared policy.
        let edit_sequence = last_invalid_seq.unwrap_or_else(|| {
            invalid_seq_boundary(SequenceDomainChange::Truncate, current.last_sequence + 1)
                .unwrap_or_default()
        });

        match truncate_kind {
            TruncateKind::All {
                truncated_entry_id,
                truncated_sequence,
            } => {
                let memtable_builder = version.memtables.mutable.memtable_builder().clone();
                let new_mutable = Self::new_mutable_from_version(
                    &version,
                    version.metadata.clone(),
                    memtable_builder,
                );
                let new_version = Arc::new(
                    VersionBuilder::from_version(version)
                        .memtables(MemtableVersion::new(new_mutable))
                        .clear_files()
                        .flushed_entry_id(truncated_entry_id)
                        .flushed_sequence(truncated_sequence)
                        .truncated_entry_id(Some(truncated_entry_id))
                        .last_invalid_seq(edit_sequence)
                        .build(),
                );

                let mut version_data = self.data.write().unwrap();
                version_data.version.ssts.mark_all_deleted();
                // Publish the boundary and the commit watermark in one step: a reader
                // that observes the new `Version` must also observe `H >= B`,
                // otherwise its recovery round would immediately invalidate itself.
                version_data.committed_sequence =
                    version_data.committed_sequence.max(edit_sequence);
                version_data.last_sequence = version_data
                    .last_sequence
                    .max(version_data.committed_sequence);
                version_data.version = new_version;
            }
            TruncateKind::Partial { files_to_remove } => {
                let new_version = Arc::new(
                    VersionBuilder::from_version(version)
                        .remove_files(files_to_remove.into_iter())
                        .last_invalid_seq(edit_sequence)
                        .build(),
                );

                let mut version_data = self.data.write().unwrap();
                // notice since it's partial, no need to mark all files as deleted
                version_data.committed_sequence =
                    version_data.committed_sequence.max(edit_sequence);
                version_data.last_sequence = version_data
                    .last_sequence
                    .max(version_data.committed_sequence);
                version_data.version = new_version;
            }
        };
    }

    /// Discards all memtables while preserving persisted SST files.
    ///
    /// Like [Self::truncate] this drops rows, so it allocates an edit boundary
    /// from `last_sequence + 1`. The allocation is in-memory only: the
    /// persisted discard action is flush-shaped and carries no boundary, so after
    /// a reload the boundary falls back to the manifest value (unknown => 0).
    pub(crate) fn discard_unflushed(
        &self,
        discarded_entry_id: EntryId,
        discarded_sequence: SequenceNumber,
        last_invalid_seq: Option<SequenceNumber>,
    ) {
        let current = self.current();
        let version = current.version;
        // The discard action records the boundary through the shared policy; fall
        // back to the policy allocation when the action carries none.
        let edit_sequence = last_invalid_seq.unwrap_or_else(|| {
            invalid_seq_boundary(
                SequenceDomainChange::DiscardUnflushed,
                current.last_sequence + 1,
            )
            .unwrap_or_default()
        });
        let memtable_builder = version.memtables.mutable.memtable_builder().clone();
        let new_mutable =
            Self::new_mutable_from_version(&version, version.metadata.clone(), memtable_builder);
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .memtables(MemtableVersion::new(new_mutable))
                .flushed_entry_id(discarded_entry_id)
                .flushed_sequence(discarded_sequence)
                .last_invalid_seq(edit_sequence)
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        // Publish the boundary and the commit watermark together, so a recovery round
        // that observes the new `Version` also observes `H >= B`.
        version_data.committed_sequence = version_data.committed_sequence.max(edit_sequence);
        version_data.last_sequence = version_data
            .last_sequence
            .max(version_data.committed_sequence);
        version_data.version = new_version;
    }

    /// Overwrites the current version with a new version.
    pub(crate) fn overwrite_current(&self, version: VersionRef) {
        let mut version_data = self.data.write().unwrap();
        version_data.last_sequence = version_data.last_sequence.max(version.flushed_sequence);
        version_data.version = version;
    }

    fn new_mutable_from_version(
        version: &Version,
        metadata: RegionMetadataRef,
        memtable_builder: MemtableBuilderRef,
    ) -> TimePartitions {
        TimePartitions::new(
            metadata,
            memtable_builder,
            version.memtables.mutable.next_memtable_id(),
            Some(version.memtables.mutable.part_duration()),
        )
    }
}

pub(crate) type VersionControlRef = Arc<VersionControl>;

/// Data of [VersionControl].
#[derive(Debug, Clone)]
pub(crate) struct VersionControlData {
    /// Latest version.
    pub(crate) version: VersionRef,
    /// Last allocated sequence number.
    pub(crate) last_sequence: SequenceNumber,
    /// Sequence number of last committed data.
    ///
    /// Starts from 1 (zero means no data).
    pub(crate) committed_sequence: SequenceNumber,
    /// Last WAL entry Id.
    ///
    /// Starts from 1 (zero means no data).
    pub(crate) last_entry_id: EntryId,
    /// Marker of whether this region is dropped/dropping
    pub(crate) is_dropped: bool,
}

impl VersionControlData {
    /// Approximate timeseries count in current version.
    pub(crate) fn series_count(&self) -> usize {
        self.version.memtables.mutable.series_count()
    }
}

/// Static metadata of a region.
#[derive(Clone, Debug)]
pub(crate) struct Version {
    /// Metadata of the region.
    ///
    /// Altering metadata isn't frequent, storing metadata in Arc to allow sharing
    /// metadata and reuse metadata when creating a new `Version`.
    pub(crate) metadata: RegionMetadataRef,
    /// Mutable and immutable memtables.
    ///
    /// Wrapped in Arc to make clone of `Version` much cheaper.
    pub(crate) memtables: MemtableVersionRef,
    /// SSTs of the region.
    pub(crate) ssts: SstVersionRef,
    /// Inclusive max WAL entry id of flushed data.
    pub(crate) flushed_entry_id: EntryId,
    /// Inclusive max sequence of flushed data.
    pub(crate) flushed_sequence: SequenceNumber,
    /// Latest entry id during the truncating table.
    ///
    /// Used to check if it is a flush task during the truncating table.
    pub(crate) truncated_entry_id: Option<EntryId>,
    /// Inferred compaction time window from flush.
    ///
    /// If compaction options contain a time window, it will overwrite this value
    /// when creating a new version from the [VersionBuilder].
    pub(crate) compaction_time_window: Option<Duration>,
    /// Options of the region.
    pub(crate) options: RegionOptions,
    /// WAL ranges the region still has to recover.
    pub(crate) pending_tails: Vec<PendingTail>,
    /// `last_invalid_seq`: latest publication boundary after which an old
    /// checkpoint can no longer explain the currently visible data.
    ///
    /// Advanced on row re-homing (a generic region edit such as `ingest_sst`, a
    /// staging apply, a region copy), data deletion or replacement (truncation,
    /// discarding unflushed rows, an edit that replaces an existing file id), and a
    /// visible partition-range change. Not advanced by flush, compaction, index
    /// updates, or pure new-data writes. Exact incremental reads require
    /// `C >= last_invalid_seq`.
    ///
    /// It is kept apart from [VersionControlData::committed_sequence], which
    /// ordinary writes advance. Publishing paths allocate `B = last_sequence + 1`
    /// so it is strictly above every sequence already allocated or published, and
    /// the region manifest keeps the max value, so the boundary survives an
    /// open/reload. Zero means unknown or never published, which never invalidates
    /// a cursor.
    pub(crate) last_invalid_seq: SequenceNumber,
}

impl Version {
    pub(crate) fn has_pending_tails(&self) -> bool {
        !self.pending_tails.is_empty()
    }

    pub(crate) fn ensure_no_pending_tails(&self) -> Result<()> {
        ensure!(
            !self.has_pending_tails(),
            RegionPendingTailsSnafu {
                region_id: self.metadata.region_id,
            }
        );
        Ok(())
    }

    /// Returns [Version::last_invalid_seq]: `B`.
    ///
    /// Zero means unknown or never published since the region was created. An exact
    /// incremental read requires `C >= B`; a reader below it cannot resume.
    pub(crate) fn last_invalid_seq(&self) -> SequenceNumber {
        self.last_invalid_seq
    }
}

pub(crate) type VersionRef = Arc<Version>;

/// A region-mutating operation that may change which sequence domain the rows of
/// a region belong to.
///
/// Only used by [marks_sequence_domain_change], which keeps the whole policy in
/// one place. Flush, write and compaction are deliberately **absent**: they never
/// reassign row sequences and must never invalidate a cursor.
#[derive(Debug, Clone, Copy)]
pub(crate) enum SequenceDomainChange {
    /// A generic region edit, i.e. any request served by `handle_region_edit`:
    /// an import such as `ingest_sst`, a repartition, a staging apply.
    RegionEdit,
    /// A truncation of all or part of the region's data.
    Truncate,
    /// A discard of unflushed data.
    DiscardUnflushed,
    /// A partition-expression (partition range) change.
    PartitionExprChange,
}

/// Single decision point: does `change` reassign which sequence domain the
/// region's rows belong to, so that every reader cursor below the boundary it
/// allocates must be invalidated?
///
/// **This function is the only place that answers that question.** Callers that
/// publish such a change route the decision through here and record the boundary
/// only when it returns `true`. Refining the policy is a change to this function
/// only.
///
/// Conservative policy: the generic edit interface cannot prove that newly added
/// data was never consumed elsewhere (a locally re-added file keeps its physical
/// row sequences; an existing file id can be replaced by an add-only edit). Until
/// that proof is expressible, every generic region edit advances the boundary.
///
/// The edit shape is not a substitute for that proof: `files_to_remove` may be
/// empty for a destructive replacement, an assigned file `sequence` only proves
/// the rows are readable in the target domain, and `preserve_row_sequence` only
/// describes whether the physical sequence column may be trusted, not whether the
/// rows were already consumed. The price is one full rebuild on the Flow side
/// after an edit that happened to be benign.
pub(crate) fn marks_sequence_domain_change(change: SequenceDomainChange) -> bool {
    matches!(
        change,
        SequenceDomainChange::RegionEdit
            | SequenceDomainChange::Truncate
            | SequenceDomainChange::DiscardUnflushed
            | SequenceDomainChange::PartitionExprChange
    )
}

/// Returns the boundary `change` must record, given the sequence number it already
/// allocated (`last_sequence + 1`), or `None` when it leaves the region's
/// sequence domain alone.
///
/// Keeps the allocation next to the policy above so every publishing path derives
/// both from one place.
pub(crate) fn invalid_seq_boundary(
    change: SequenceDomainChange,
    allocated_sequence: SequenceNumber,
) -> Option<SequenceNumber> {
    marks_sequence_domain_change(change).then_some(allocated_sequence)
}

/// Version builder.
pub(crate) struct VersionBuilder {
    metadata: RegionMetadataRef,
    memtables: MemtableVersionRef,
    ssts: SstVersionRef,
    flushed_entry_id: EntryId,
    flushed_sequence: SequenceNumber,
    truncated_entry_id: Option<EntryId>,
    compaction_time_window: Option<Duration>,
    options: RegionOptions,
    pending_tails: Vec<PendingTail>,
    last_invalid_seq: SequenceNumber,
}

impl VersionBuilder {
    /// Returns a new builder.
    pub(crate) fn new(metadata: RegionMetadataRef, mutable: TimePartitions) -> Self {
        VersionBuilder {
            metadata: metadata.clone(),
            memtables: Arc::new(MemtableVersion::new(mutable)),
            ssts: Arc::new(SstVersion::new(metadata)),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            options: RegionOptions::default(),
            pending_tails: Vec::new(),
            last_invalid_seq: 0,
        }
    }

    /// Returns a new builder from an existing version.
    pub(crate) fn from_version(version: VersionRef) -> Self {
        VersionBuilder {
            metadata: version.metadata.clone(),
            memtables: version.memtables.clone(),
            ssts: version.ssts.clone(),
            flushed_entry_id: version.flushed_entry_id,
            flushed_sequence: version.flushed_sequence,
            truncated_entry_id: version.truncated_entry_id,
            compaction_time_window: version.compaction_time_window,
            options: version.options.clone(),
            pending_tails: version.pending_tails.clone(),
            last_invalid_seq: version.last_invalid_seq,
        }
    }

    /// Sets memtables.
    pub(crate) fn memtables(mut self, memtables: MemtableVersion) -> Self {
        self.memtables = Arc::new(memtables);
        self
    }

    /// Sets metadata.
    pub(crate) fn metadata(mut self, metadata: RegionMetadataRef) -> Self {
        if !Arc::ptr_eq(&self.metadata, &metadata) {
            Arc::make_mut(&mut self.ssts).set_metadata(metadata.clone());
        }
        self.metadata = metadata;
        self
    }

    /// Sets flushed entry id.
    pub(crate) fn flushed_entry_id(mut self, entry_id: EntryId) -> Self {
        self.flushed_entry_id = entry_id;
        self
    }

    /// Sets flushed sequence.
    pub(crate) fn flushed_sequence(mut self, sequence: SequenceNumber) -> Self {
        self.flushed_sequence = sequence;
        self
    }

    /// Sets truncated entry id.
    pub(crate) fn truncated_entry_id(mut self, entry_id: Option<EntryId>) -> Self {
        self.truncated_entry_id = entry_id;
        self
    }

    /// Sets compaction time window.
    pub(crate) fn compaction_time_window(mut self, window: Option<Duration>) -> Self {
        self.compaction_time_window = window;
        self
    }

    /// Sets options.
    pub(crate) fn options(mut self, options: RegionOptions) -> Self {
        self.options = options;
        self
    }

    pub(crate) fn pending_tails(mut self, pending_tails: Vec<PendingTail>) -> Self {
        self.pending_tails = pending_tails;
        self
    }

    /// Sets [Version::last_invalid_seq].
    ///
    /// Callers pass the boundary an edit/truncation/copy allocated, never a
    /// flush, write or compaction watermark. The value is absorbed with `max`, so
    /// replaying an older boundary cannot lower it.
    pub(crate) fn last_invalid_seq(mut self, sequence: SequenceNumber) -> Self {
        self.last_invalid_seq = self.last_invalid_seq.max(sequence);
        self
    }

    /// Apply edit to the builder.
    pub(crate) fn apply_edit(mut self, edit: RegionEdit, file_purger: FilePurgerRef) -> Self {
        if let Some(entry_id) = edit.flushed_entry_id {
            self.flushed_entry_id = self.flushed_entry_id.max(entry_id);
        }
        if let Some(sequence) = edit.flushed_sequence {
            self.flushed_sequence = self.flushed_sequence.max(sequence);
        }
        if let Some(sequence) = edit.last_invalid_seq {
            // Only edits that may reassign row sequences record this field: flush,
            // write and compaction edits leave it `None`, so they can never
            // invalidate a cursor. Take the max so replaying an older edit cannot
            // lower it.
            self.last_invalid_seq = self.last_invalid_seq.max(sequence);
        }
        if let Some(window) = edit.compaction_time_window {
            self.compaction_time_window = Some(window);
        }
        if let Some(pending_tails) = edit.wal_recovery.pending_tails {
            self.pending_tails = pending_tails;
        }
        if !edit.files_to_add.is_empty() || !edit.files_to_remove.is_empty() {
            let mut ssts = (*self.ssts).clone();
            ssts.add_files(file_purger, edit.files_to_add.into_iter());
            ssts.remove_files(edit.files_to_remove.into_iter());
            self.ssts = Arc::new(ssts);
        }

        self
    }

    /// Remove memtables from the builder.
    pub(crate) fn remove_memtables(mut self, ids: &[MemtableId]) -> Self {
        if !ids.is_empty() {
            let mut memtables = (*self.memtables).clone();
            memtables.remove_memtables(ids);
            self.memtables = Arc::new(memtables);
        }
        self
    }

    /// Add files to the builder.
    pub(crate) fn add_files(
        mut self,
        file_purger: FilePurgerRef,
        files: impl Iterator<Item = FileMeta>,
    ) -> Self {
        let mut ssts = (*self.ssts).clone();
        ssts.add_files(file_purger, files);
        self.ssts = Arc::new(ssts);

        self
    }

    pub(crate) fn remove_files(mut self, files: impl Iterator<Item = FileMeta>) -> Self {
        let mut ssts = (*self.ssts).clone();
        ssts.remove_files(files);
        self.ssts = Arc::new(ssts);

        self
    }

    /// Clear all files in the builder.
    pub(crate) fn clear_files(mut self) -> Self {
        self.ssts = Arc::new(SstVersion::new(self.metadata.clone()));
        self
    }

    /// Builds a new [Version] from the builder.
    /// It overwrites the window size by compaction option.
    pub(crate) fn build(self) -> Version {
        let compaction_time_window = self
            .options
            .compaction
            .time_window()
            .or(self.compaction_time_window);
        if self.compaction_time_window.is_some()
            && compaction_time_window != self.compaction_time_window
        {
            info!(
                "VersionBuilder overwrites region compaction time window from {:?} to {:?}, region: {}",
                self.compaction_time_window, compaction_time_window, self.metadata.region_id
            );
        }

        Version {
            metadata: self.metadata,
            memtables: self.memtables,
            ssts: self.ssts,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window,
            options: self.options,
            pending_tails: self.pending_tails,
            last_invalid_seq: self.last_invalid_seq,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::version_util::VersionControlBuilder;

    #[test]
    fn test_sequence_watermarks_are_monotonic() {
        let builder = VersionControlBuilder::new();
        let mut version = builder.build_version();
        version.flushed_sequence = 8;
        version.flushed_entry_id = 4;
        let vc = VersionControl::new(version);
        assert_eq!(
            (vc.current().last_sequence, vc.committed_sequence()),
            (8, 8)
        );

        vc.update_last_sequence(20);
        vc.update_last_sequence(9);
        vc.set_committed_sequence(10);
        vc.set_committed_sequence(9);
        assert_eq!(
            (vc.current().last_sequence, vc.committed_sequence()),
            (20, 10)
        );
        assert_eq!(vc.current().last_entry_id, 4);
        vc.set_sequence_and_entry_id(12, 5);
        vc.set_sequence_and_entry_id(4, 6);
        assert_eq!(
            (vc.current().last_sequence, vc.committed_sequence()),
            (20, 12)
        );
        assert_eq!(vc.current().version.flushed_sequence, 8);

        for sequence in [25, 11] {
            vc.apply_edit(
                Some(RegionEdit {
                    files_to_add: vec![],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: Some(sequence),
                    wal_recovery: Default::default(),
                    last_invalid_seq: None,
                }),
                &[],
                builder.file_purger(),
            );
            assert_eq!(
                (vc.current().last_sequence, vc.committed_sequence()),
                (25, 25)
            );
            assert_eq!(vc.current().last_entry_id, 6);
        }
    }

    #[test]
    fn test_last_invalid_seq_tracks_edits_only() {
        let builder = VersionControlBuilder::new();
        let mut version = builder.build_version();
        version.flushed_sequence = 8;
        let vc = VersionControl::new(version);
        assert_eq!(0, vc.current().version.last_invalid_seq());

        // Ordinary write watermarks and flushes never advance the boundary.
        vc.update_last_sequence(20);
        vc.set_committed_sequence(30);
        assert_eq!(0, vc.current().version.last_invalid_seq());

        // Truncation drops rows, so it allocates `last_sequence + 1` (31) when the
        // action carries no boundary.
        vc.truncate(
            TruncateKind::Partial {
                files_to_remove: vec![],
            },
            None,
        );
        assert_eq!(31, vc.current().version.last_invalid_seq());
        // The publish also raises the live watermark to the boundary, so a reader
        // that observes the new version observes `H >= B`.
        assert_eq!(31, vc.current().last_sequence);

        // A commit watermark alone never records a boundary: only the explicit
        // `last_invalid_seq` field of an edit does.
        vc.apply_edit(
            Some(RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: Some(50),
                wal_recovery: Default::default(),
                last_invalid_seq: None,
            }),
            &[],
            builder.file_purger(),
        );
        assert_eq!(31, vc.current().version.last_invalid_seq());
        assert_eq!(50, vc.committed_sequence());

        // Edits record it monotonically: a smaller later value cannot lower it.
        for sequence in [25, 11] {
            vc.apply_edit(
                Some(RegionEdit {
                    files_to_add: vec![],
                    files_to_remove: vec![],
                    timestamp_ms: None,
                    compaction_time_window: None,
                    flushed_entry_id: None,
                    flushed_sequence: None,
                    committed_sequence: Some(sequence),
                    wal_recovery: Default::default(),
                    last_invalid_seq: Some(sequence),
                }),
                &[],
                builder.file_purger(),
            );
        }
        assert_eq!(31, vc.current().version.last_invalid_seq());

        // A later, larger allocation advances it.
        vc.apply_edit(
            Some(RegionEdit {
                files_to_add: vec![],
                files_to_remove: vec![],
                timestamp_ms: None,
                compaction_time_window: None,
                flushed_entry_id: None,
                flushed_sequence: None,
                committed_sequence: Some(40),
                wal_recovery: Default::default(),
                last_invalid_seq: Some(40),
            }),
            &[],
            builder.file_purger(),
        );
        assert_eq!(40, vc.current().version.last_invalid_seq());

        // Advancing the live watermark afterwards leaves the boundary alone.
        vc.set_committed_sequence(60);
        assert_eq!(40, vc.current().version.last_invalid_seq());
        assert_eq!(60, vc.committed_sequence());
    }
}
