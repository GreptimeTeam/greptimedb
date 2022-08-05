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
use std::time::Duration;

use store_api::manifest::ManifestVersion;
use store_api::storage::{SchemaRef, SequenceNumber};

use crate::memtable::{MemtableId, MemtableSchema, MemtableSet, MemtableVersion};
use crate::metadata::RegionMetadataRef;
use crate::sst::LevelMetas;
use crate::sst::{FileHandle, FileMeta};
use crate::sync::CowCell;

/// Default bucket duration: 2 Hours.
const DEFAULT_BUCKET_DURATION: Duration = Duration::from_secs(3600 * 2);

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
            committed_sequence: AtomicU64::new(0),
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
        self.committed_sequence.load(Ordering::Acquire)
    }

    /// Set committed sequence to `value`.
    ///
    /// External synchronization is required to ensure only one thread can update the
    /// last sequence.
    #[inline]
    pub fn set_committed_sequence(&self, value: SequenceNumber) {
        // Release ordering should be enough to guarantee sequence is updated at last.
        self.committed_sequence.store(value, Ordering::Release);
    }

    /// Add mutable memtables and commit.
    ///
    /// # Panics
    /// See [MemtableVersion::add_mutable](MemtableVersion::add_mutable).
    pub fn add_mutable(&self, memtables_to_add: MemtableSet) {
        let mut version_to_update = self.version.lock();

        let memtable_version = version_to_update.memtables();
        let merged = memtable_version.add_mutable(memtables_to_add);
        version_to_update.memtables = Arc::new(merged);

        version_to_update.commit();
    }

    /// Freeze all mutable memtables.
    pub fn freeze_mutable(&self) {
        let mut version_to_update = self.version.lock();

        let memtable_version = version_to_update.memtables();
        let freezed = memtable_version.freeze_mutable();
        version_to_update.memtables = Arc::new(freezed);

        version_to_update.commit();
    }

    pub fn apply_edit(&self, edit: VersionEdit) {
        let mut version_to_update = self.version.lock();
        version_to_update.apply_edit(edit);
        version_to_update.commit();
    }
}

#[derive(Debug)]
pub struct VersionEdit {
    pub files_to_add: Vec<FileMeta>,
    pub flushed_sequence: Option<SequenceNumber>,
    pub manifest_version: ManifestVersion,
    pub max_memtable_id: Option<MemtableId>,
}

pub type VersionControlRef = Arc<VersionControl>;
pub type VersionRef = Arc<Version>;
type MemtableVersionRef = Arc<MemtableVersion>;
type LevelMetasRef = Arc<LevelMetas>;

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
    pub fn new(metadata: RegionMetadataRef) -> Version {
        Version::with_manifest_version(metadata, 0)
    }

    /// Create a new `Version` with given `metadata` and initial `manifest_version`.
    pub fn with_manifest_version(
        metadata: RegionMetadataRef,
        manifest_version: ManifestVersion,
    ) -> Version {
        Version {
            metadata,
            memtables: Arc::new(MemtableVersion::new()),
            ssts: Arc::new(LevelMetas::new()),
            flushed_sequence: 0,
            manifest_version,
        }
    }

    #[inline]
    pub fn metadata(&self) -> &RegionMetadataRef {
        &self.metadata
    }

    #[inline]
    pub fn schema(&self) -> &SchemaRef {
        &self.metadata.schema
    }

    #[inline]
    pub fn mutable_memtables(&self) -> &MemtableSet {
        self.memtables.mutable_memtables()
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

    /// Returns duration used to partition the memtables and ssts by time.
    pub fn bucket_duration(&self) -> Duration {
        DEFAULT_BUCKET_DURATION
    }

    #[inline]
    pub fn memtable_schema(&self) -> MemtableSchema {
        MemtableSchema::new(self.metadata.columns_row_key.clone())
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

        let handles_to_add = edit.files_to_add.into_iter().map(FileHandle::new);
        let merged_ssts = self.ssts.merge(handles_to_add);

        self.ssts = Arc::new(merged_ssts);
    }

    #[inline]
    pub fn manifest_version(&self) -> ManifestVersion {
        self.manifest_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RegionMetadata;
    use crate::test_util::descriptor_util::RegionDescBuilder;

    fn new_version_control() -> VersionControl {
        let desc = RegionDescBuilder::new("version-test")
            .enable_version_column(false)
            .build();
        let metadata: RegionMetadata = desc.try_into().unwrap();

        let version = Version::new(Arc::new(metadata));
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
