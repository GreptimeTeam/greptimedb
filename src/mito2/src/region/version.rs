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

use store_api::metadata::RegionMetadataRef;
use store_api::storage::SequenceNumber;

use crate::manifest::action::RegionEdit;
use crate::memtable::version::{MemtableVersion, MemtableVersionRef};
use crate::memtable::{MemtableBuilderRef, MemtableId, MemtableRef};
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
                committed_sequence: flushed_sequence,
                last_entry_id: flushed_entry_id,
                is_dropped: false,
            }),
        }
    }

    /// Returns current copy of data.
    pub(crate) fn current(&self) -> VersionControlData {
        self.data.read().unwrap().clone()
    }

    /// Updates committed sequence and entry id.
    pub(crate) fn set_sequence_and_entry_id(&self, seq: SequenceNumber, entry_id: EntryId) {
        let mut data = self.data.write().unwrap();
        data.committed_sequence = seq;
        data.last_entry_id = entry_id;
    }

    /// Freezes the mutable memtable if it is not empty.
    pub(crate) fn freeze_mutable(&self, builder: &MemtableBuilderRef) {
        let version = self.current().version;
        if version.memtables.mutable.is_empty() {
            return;
        }
        let new_mutable = builder.build(&version.metadata);
        // Safety: Immutable memtable is None.
        let new_memtables = version.memtables.freeze_mutable(new_mutable).unwrap();
        // Create a new version with memtable switched.
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .memtables(new_memtables)
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Apply edit to current version.
    pub(crate) fn apply_edit(
        &self,
        edit: RegionEdit,
        memtables_to_remove: &[MemtableId],
        purger: FilePurgerRef,
    ) {
        let version = self.current().version;
        let new_version = Arc::new(
            VersionBuilder::from_version(version)
                .apply_edit(edit, purger)
                .remove_memtables(memtables_to_remove)
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version = new_version;
    }

    /// Mark all opened files as deleted and set the delete marker in [VersionControlData]
    pub(crate) fn mark_dropped(&self, memtable_builder: &MemtableBuilderRef) {
        let version = self.current().version;
        let new_mutable = memtable_builder.build(&version.metadata);

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
    pub(crate) fn alter_schema(&self, metadata: RegionMetadataRef, builder: &MemtableBuilderRef) {
        let new_mutable = builder.build(&metadata);
        let version = self.current().version;
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

    /// Truncate current version.
    pub(crate) fn truncate(
        &self,
        truncated_entry_id: EntryId,
        truncated_sequence: SequenceNumber,
        memtable_builder: &MemtableBuilderRef,
    ) {
        let version = self.current().version;

        let new_mutable = memtable_builder.build(&version.metadata);
        let new_version = Arc::new(
            VersionBuilder::new(version.metadata.clone(), new_mutable)
                .flushed_entry_id(truncated_entry_id)
                .flushed_sequence(truncated_sequence)
                .truncated_entry_id(Some(truncated_entry_id))
                .build(),
        );

        let mut version_data = self.data.write().unwrap();
        version_data.version.ssts.mark_all_deleted();
        version_data.version = new_version;
    }
}

pub(crate) type VersionControlRef = Arc<VersionControl>;

/// Data of [VersionControl].
#[derive(Debug, Clone)]
pub(crate) struct VersionControlData {
    /// Latest version.
    pub(crate) version: VersionRef,
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
    /// Inferred compaction time window.
    pub(crate) compaction_time_window: Option<Duration>,
    /// Options of the region.
    pub(crate) options: RegionOptions,
}

pub(crate) type VersionRef = Arc<Version>;

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
}

impl VersionBuilder {
    /// Returns a new builder.
    pub(crate) fn new(metadata: RegionMetadataRef, mutable: MemtableRef) -> Self {
        VersionBuilder {
            metadata,
            memtables: Arc::new(MemtableVersion::new(mutable)),
            ssts: Arc::new(SstVersion::new()),
            flushed_entry_id: 0,
            flushed_sequence: 0,
            truncated_entry_id: None,
            compaction_time_window: None,
            options: RegionOptions::default(),
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
        }
    }

    /// Sets memtables.
    pub(crate) fn memtables(mut self, memtables: MemtableVersion) -> Self {
        self.memtables = Arc::new(memtables);
        self
    }

    /// Sets metadata.
    pub(crate) fn metadata(mut self, metadata: RegionMetadataRef) -> Self {
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

    /// Sets truncated entty id.
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

    /// Apply edit to the builder.
    pub(crate) fn apply_edit(mut self, edit: RegionEdit, file_purger: FilePurgerRef) -> Self {
        if let Some(entry_id) = edit.flushed_entry_id {
            self.flushed_entry_id = self.flushed_entry_id.max(entry_id);
        }
        if let Some(sequence) = edit.flushed_sequence {
            self.flushed_sequence = self.flushed_sequence.max(sequence);
        }
        if let Some(window) = edit.compaction_time_window {
            self.compaction_time_window = Some(window);
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

    /// Builds a new [Version] from the builder.
    pub(crate) fn build(self) -> Version {
        Version {
            metadata: self.metadata,
            memtables: self.memtables,
            ssts: self.ssts,
            flushed_entry_id: self.flushed_entry_id,
            flushed_sequence: self.flushed_sequence,
            truncated_entry_id: self.truncated_entry_id,
            compaction_time_window: self.compaction_time_window,
            options: self.options,
        }
    }
}
