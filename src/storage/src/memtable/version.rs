use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::time::Duration;

use common_time::RangeMillis;
use snafu::Snafu;

use crate::flush::MemtableWithMeta;
use crate::memtable::MemtableRef;

#[derive(Debug, Snafu)]
#[snafu(display("Failed to freeze mutable memtable, immutable memtable already exists"))]
pub struct FreezeError;

/// A version of all memtables.
///
/// This structure is immutable now.
#[derive(Default)]
pub struct MemtableVersion {
    mutable: MemtableSet,
    /// Immutable memtables.
    // TODO(yingwen): [flush] Use Vec<MemtableSet> to hold immutable memtables.
    immem: Option<MemtableSet>,
}

impl MemtableVersion {
    pub fn new() -> MemtableVersion {
        MemtableVersion::default()
    }

    pub fn mutable_memtables(&self) -> &MemtableSet {
        &self.mutable
    }

    // FIXME(yingwen): [flush] Always freeze memtables.
    /// Clone current `MemtableVersion`, try to freeze mutable memtables in the new
    /// version then returns that version.
    ///
    /// Returns `Err` if immutable memtables already exists.
    pub fn try_freeze_mutable(&self) -> Result<MemtableVersion, FreezeError> {
        match self.immem {
            Some(_) => FreezeSnafu {}.fail(),
            None => Ok(MemtableVersion {
                mutable: MemtableSet::new(),
                // TODO(yingwen): Consider using Arc<MemtableSet> since this structure is immutable.
                immem: Some(self.mutable.clone()),
            }),
        }
    }

    pub fn mutable_bytes_allocated(&self) -> usize {
        self.mutable.bytes_allocated()
    }

    pub fn total_bytes_allocated(&self) -> usize {
        self.immem.as_ref().map_or(0, MemtableSet::bytes_allocated) + self.mutable.bytes_allocated()
    }

    /// Creates a new `MemtableVersion` that contains memtables both in this and `other`.
    ///
    /// # Panics
    /// Panics if there are memtables with same time ranges.
    pub fn add_mutable(&self, other: MemtableSet) -> MemtableVersion {
        let mutable = self.mutable.add(other);

        Self {
            mutable,
            immem: self.immem.clone(),
        }
    }

    #[inline]
    pub fn memtables_to_flush(&self, bucket_duration: Duration) -> Vec<MemtableWithMeta> {
        self.immem
            .as_ref()
            .map(|immem| immem.to_memtable_with_metas(bucket_duration))
            .unwrap_or_default()
    }
}

// We use a new type to order time ranges by (end, start).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeKey(RangeMillis);

impl Ord for RangeKey {
    fn cmp(&self, other: &RangeKey) -> Ordering {
        self.0
            .end()
            .cmp(other.0.end())
            .then_with(|| self.0.start().cmp(other.0.start()))
    }
}

impl PartialOrd for RangeKey {
    fn partial_cmp(&self, other: &RangeKey) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Collection of mutable memtables.
///
/// Memtables are partitioned by their time range. Caller should ensure
/// there are no overlapped ranges and all ranges are aligned by same
/// bucket duration.
#[derive(Default, Clone)]
pub struct MemtableSet {
    memtables: BTreeMap<RangeKey, MemtableRef>,
}

impl MemtableSet {
    fn new() -> MemtableSet {
        MemtableSet::default()
    }

    /// Get memtable by time range.
    ///
    /// The range must exactly equal to the range of the memtable, otherwise `None`
    /// is returned.
    pub fn get_by_range(&self, range: &RangeMillis) -> Option<&MemtableRef> {
        let range_key = RangeKey(*range);
        self.memtables.get(&range_key)
    }

    /// Insert a new memtable.
    ///
    /// # Panics
    /// Panics if memtable with same range already exists.
    pub fn insert(&mut self, range: RangeMillis, mem: MemtableRef) {
        let old = self.memtables.insert(RangeKey(range), mem);
        assert!(old.is_none());
    }

    pub fn is_empty(&self) -> bool {
        self.memtables.is_empty()
    }

    pub fn bytes_allocated(&self) -> usize {
        self.memtables.values().map(|m| m.bytes_allocated()).sum()
    }

    /// Creates a new `MemtableSet` that contains memtables both in `self` and
    /// `other`, let `self` unchanged.
    ///
    /// # Panics
    /// Panics if there are memtables with same time ranges.
    pub fn add(&self, _other: MemtableSet) -> MemtableSet {
        // TODO(yingwen): [flush] Add mutable memtables.
        unimplemented!()
    }

    pub fn to_memtable_with_metas(&self, _bucket_duration: Duration) -> Vec<MemtableWithMeta> {
        // TODO(yingwen): [flush] Create MemtableWithMeta vec.
        unimplemented!()
    }
}
