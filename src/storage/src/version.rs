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

//! Version control of storage.
//!
//! To read latest data from `VersionControl`, we need to
//! 1. Acquire `Version` from `VersionControl`.
//! 2. Then acquire last sequence.
//!
//! Reason: data may be flushed/compacted and some data with old sequence may be removed
//! and became invisible between step 1 and 2, so need to acquire version at first.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common_telemetry::{debug, info};
use store_api::manifest::ManifestVersion;
use store_api::storage::{SchemaRef, SequenceNumber};

use crate::file_purger::FilePurgerRef;
use crate::memtable::{MemtableId, MemtableRef, MemtableVersion};
use crate::metadata::RegionMetadataRef;
use crate::schema::RegionSchemaRef;
use crate::sst::{AccessLayerRef, FileMeta, LevelMetas};
use crate::sync::CowCell;
pub const INIT_COMMITTED_SEQUENCE: u64 = 0;

/// Controls version of in memory state for a region.
#[derive(Debug)]
pub struct VersionControl {
    // TODO(yingwen): If all modification to version must acquire the region writer lock first,
    // then we may just use ArcSwap to hold version. But some operations may only require the
    // version lock, instead of the writer lock, since we can use the version lock the protect
    // the read-modify-write of version.
    version: CowCell<Version>,
    /// Latest sequence that is committed and visible to user.
    committed_sequence: AtomicU64,
}

impl VersionControl {
    /// Construct a new version control from existing `version`.
    pub fn with_version(version: Version) -> VersionControl {
        VersionControl {
            version: CowCell::new(version),
            committed_sequence: AtomicU64::new(INIT_COMMITTED_SEQUENCE),
        }
    }

    /// Returns current version.
    #[inline]
    pub fn current(&self) -> VersionRef {
        self.version.get()
    }

    #[inline]
    pub fn current_manifest_version(&self) -> ManifestVersion {
        self.current().manifest_version
    }

    /// Metadata of current version.
    pub fn metadata(&self) -> RegionMetadataRef {
        let version = self.current();
        version.metadata.clone()
    }

    #[inline]
    pub fn committed_sequence(&self) -> SequenceNumber {
        self.committed_sequence.load(Ordering::Relaxed)
    }

    /// Set committed sequence to `value`.
    ///
    /// External synchronization is required to ensure only one thread can update the
    /// last sequence.
    #[inline]
    pub fn set_committed_sequence(&self, value: SequenceNumber) {
        // Relaxed ordering is enough for this update as this method requires external synchoronization.
        self.committed_sequence.store(value, Ordering::Relaxed);
    }

    /// Freeze all mutable memtables.
    pub fn freeze_mutable(&self, new_memtable: MemtableRef) {
        let mut version_to_update = self.version.lock();

        let memtable_version = version_to_update.memtables();
        let freezed = memtable_version.freeze_mutable(new_memtable);
        version_to_update.memtables = Arc::new(freezed);

        version_to_update.commit();
    }

    /// Apply [VersionEdit] to the version.
    pub fn apply_edit(&self, edit: VersionEdit) {
        let mut version_to_update = self.version.lock();
        version_to_update.apply_edit(edit);
        version_to_update.commit();
    }

    /// Freeze all mutable memtables and then apply the new metadata to the version.
    pub fn freeze_mutable_and_apply_metadata(
        &self,
        metadata: RegionMetadataRef,
        manifest_version: ManifestVersion,
        mutable_memtable: MemtableRef,
    ) {
        let mut version_to_update = self.version.lock();

        let memtable_version = version_to_update.memtables();
        // When applying metadata, mutable memtable set might be empty and there is no
        // need to freeze it.
        let freezed = memtable_version.freeze_mutable(mutable_memtable);
        version_to_update.memtables = Arc::new(freezed);

        version_to_update.apply_metadata(metadata, manifest_version);
        version_to_update.commit();
    }
}

#[derive(Debug)]
pub struct VersionEdit {
    pub files_to_add: Vec<FileMeta>,
    pub files_to_remove: Vec<FileMeta>,
    pub flushed_sequence: Option<SequenceNumber>,
    pub manifest_version: ManifestVersion,
    pub max_memtable_id: Option<MemtableId>,
}

pub type VersionControlRef = Arc<VersionControl>;
pub type VersionRef = Arc<Version>;
type MemtableVersionRef = Arc<MemtableVersion>;
pub type LevelMetasRef = Arc<LevelMetas>;

/// Version contains metadata and state of region.
#[derive(Clone, Debug)]
pub struct Version {
    /// Metadata of the region.
    ///
    /// Altering metadata isn't frequent, storing metadata in Arc to allow sharing
    /// metadata and reuse metadata when creating a new `Version`.
    metadata: RegionMetadataRef,
    /// Mutable and immutable memtables.
    ///
    /// Wrapped in Arc to make clone of `Version` much cheaper.
    memtables: MemtableVersionRef,
    /// SSTs of the region.
    ssts: LevelMetasRef,
    /// Inclusive max sequence of flushed data.
    flushed_sequence: SequenceNumber,
    /// Current version of manifest.
    manifest_version: ManifestVersion,
    // TODO(yingwen): Maybe also store last sequence to this version when switching
    // version, so we can know the newest data can read from this version.
}

impl Version {
    /// Create a new `Version` with given `metadata`.
    #[cfg(test)]
    pub fn new(metadata: RegionMetadataRef, memtable: MemtableRef) -> Version {
        let sst_layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer) as Arc<_>;
        let file_purger = Arc::new(crate::scheduler::LocalScheduler::new(
            crate::scheduler::SchedulerConfig::default(),
            crate::file_purger::noop::NoopFilePurgeHandler,
        ));
        Version::with_manifest_version(metadata, 0, memtable, sst_layer, file_purger)
    }

    /// Create a new `Version` with given `metadata` and initial `manifest_version`.
    pub fn with_manifest_version(
        metadata: RegionMetadataRef,
        manifest_version: ManifestVersion,
        mutable_memtable: MemtableRef,
        sst_layer: AccessLayerRef,
        file_purger: FilePurgerRef,
    ) -> Version {
        Version {
            metadata,
            memtables: Arc::new(MemtableVersion::new(mutable_memtable)),
            ssts: Arc::new(LevelMetas::new(sst_layer, file_purger)),
            flushed_sequence: 0,
            manifest_version,
        }
    }

    #[inline]
    pub fn metadata(&self) -> &RegionMetadataRef {
        &self.metadata
    }

    #[inline]
    pub fn schema(&self) -> &RegionSchemaRef {
        self.metadata.schema()
    }

    #[inline]
    pub fn user_schema(&self) -> &SchemaRef {
        self.metadata.user_schema()
    }

    #[inline]
    pub fn mutable_memtable(&self) -> &MemtableRef {
        self.memtables.mutable_memtable()
    }

    #[inline]
    pub fn memtables(&self) -> &MemtableVersionRef {
        &self.memtables
    }

    #[inline]
    pub fn ssts(&self) -> &LevelMetasRef {
        &self.ssts
    }

    #[inline]
    pub fn flushed_sequence(&self) -> SequenceNumber {
        self.flushed_sequence
    }

    pub fn apply_checkpoint(
        &mut self,
        flushed_sequence: Option<SequenceNumber>,
        manifest_version: ManifestVersion,
        files: impl Iterator<Item = FileMeta>,
    ) {
        self.flushed_sequence = flushed_sequence.unwrap_or(self.flushed_sequence);
        self.manifest_version = manifest_version;
        let ssts = self.ssts.merge(files, std::iter::empty());
        info!(
            "After applying checkpoint, region: {}, id: {}, flushed_sequence: {}, manifest_version: {}",
            self.metadata.name(),
            self.metadata.id(),
            self.flushed_sequence,
            self.manifest_version,
        );

        self.ssts = Arc::new(ssts);
    }

    pub fn apply_edit(&mut self, edit: VersionEdit) {
        let flushed_sequence = edit.flushed_sequence.unwrap_or(self.flushed_sequence);
        if self.flushed_sequence < flushed_sequence {
            self.flushed_sequence = flushed_sequence;
        }
        if self.manifest_version < edit.manifest_version {
            self.manifest_version = edit.manifest_version;
        }

        if let Some(max_memtable_id) = edit.max_memtable_id {
            // Remove flushed memtables
            let memtable_version = self.memtables();
            let removed = memtable_version.remove_immutables(max_memtable_id);
            self.memtables = Arc::new(removed);
        }

        let handles_to_add = edit.files_to_add.into_iter();
        let merged_ssts = self
            .ssts
            .merge(handles_to_add, edit.files_to_remove.into_iter());

        debug!(
            "After applying edit, region: {}, id: {}, SST files: {:?}",
            self.metadata.name(),
            self.metadata.id(),
            merged_ssts
        );
        self.ssts = Arc::new(merged_ssts);
    }

    /// Updates metadata of the version.
    ///
    /// # Panics
    /// Panics if `metadata.version() <= self.metadata.version()`.
    pub fn apply_metadata(
        &mut self,
        metadata: RegionMetadataRef,
        manifest_version: ManifestVersion,
    ) {
        assert!(
            metadata.version() > self.metadata.version(),
            "Updating metadata from version {} to {} is not allowed",
            self.metadata.version(),
            metadata.version()
        );

        if self.manifest_version < manifest_version {
            self.manifest_version = manifest_version;
        }

        self.metadata = metadata;
    }

    #[inline]
    pub fn manifest_version(&self) -> ManifestVersion {
        self.manifest_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::{DefaultMemtableBuilder, MemtableBuilder};
    use crate::test_util::descriptor_util::RegionDescBuilder;

    fn new_version_control() -> VersionControl {
        let desc = RegionDescBuilder::new("version-test")
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadataRef = Arc::new(desc.try_into().unwrap());
        let memtable = DefaultMemtableBuilder::default().build(metadata.schema().clone());

        let version = Version::new(metadata, memtable);
        VersionControl::with_version(version)
    }

    #[test]
    fn test_version_control() {
        let version_control = new_version_control();

        assert_eq!(0, version_control.committed_sequence());
        version_control.set_committed_sequence(12345);
        assert_eq!(12345, version_control.committed_sequence());
    }
}
