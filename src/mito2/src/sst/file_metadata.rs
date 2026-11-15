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

//! Sharing of immutable SST metadata across manifests and runtime readers.

use std::collections::hash_map::RandomState;
use std::fmt::{self, Debug, Display};
use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use intern_arc::{HashInterner, InternedHash};
use partition::expr::PartitionExpr;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use store_api::storage::TableId;

use crate::metrics::file_metadata::FILE_METADATA_COLLECTOR;
use crate::sst::file::{FileMeta, FileMetaRef, RegionFileId};

const SHARD_COUNT: usize = 32;

/// Immutable metadata, either freshly decoded or registered with an engine interner.
/// Keeping the decoded allocation avoids copying index vectors during recovery.
#[derive(Clone)]
pub struct MetadataRef<T: Eq + Hash>(MetadataRefInner<T>);

#[derive(Clone)]
enum MetadataRefInner<T: Eq + Hash> {
    Owned(Arc<T>),
    Interned(InternedHash<ScopedMetadata<T>>),
}

#[derive(PartialEq, Eq, Hash)]
struct ScopedMetadata<T: Eq + Hash> {
    table_id: TableId,
    value: Arc<T>,
}

impl<T: Eq + Hash> MetadataRef<T> {
    fn as_arc(&self) -> &Arc<T> {
        match &self.0 {
            MetadataRefInner::Owned(value) => value,
            MetadataRefInner::Interned(value) => &value.value,
        }
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        match &mut self.0 {
            MetadataRefInner::Owned(value) => Arc::get_mut(value),
            // Mutating a registered value would invalidate the hash table.
            MetadataRefInner::Interned(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(left.as_arc(), right.as_arc())
    }
}

impl<T: Eq + Hash> From<T> for MetadataRef<T> {
    fn from(value: T) -> Self {
        Self::from(Arc::new(value))
    }
}

impl<T: Eq + Hash> From<Arc<T>> for MetadataRef<T> {
    fn from(value: Arc<T>) -> Self {
        Self(MetadataRefInner::Owned(value))
    }
}

impl<T: Eq + Hash + Default> Default for MetadataRef<T> {
    fn default() -> Self {
        T::default().into()
    }
}

impl<T: Eq + Hash> Deref for MetadataRef<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.as_arc()
    }
}

impl<T: Eq + Hash> AsRef<T> for MetadataRef<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: Eq + Hash + Debug> Debug for MetadataRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_ref(), f)
    }
}

impl<T: Eq + Hash + Display> Display for MetadataRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self.as_ref(), f)
    }
}

impl<T: Eq + Hash> PartialEq for MetadataRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_arc() == other.as_arc()
    }
}

impl<T: Eq + Hash> Eq for MetadataRef<T> {}

impl<T: Eq + Hash> Hash for MetadataRef<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl<T: Eq + Hash + Serialize> Serialize for MetadataRef<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().serialize(serializer)
    }
}

impl<'de, T: Eq + Hash + Deserialize<'de>> Deserialize<'de> for MetadataRef<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Decoding must not register values in the crate's process-global pool.
        T::deserialize(deserializer).map(Self::from)
    }
}

/// Engine-local sharing. File and expression operations never hold nested locks.
/// The final registered reference removes its entry. Hash table capacity remains
/// allocated until the interner is dropped.
pub struct FileMetadataInterner {
    inner: Arc<FileMetadataInternerState>,
}

/// Shared storage observed through weak references by the metrics collector.
#[derive(Default)]
pub(crate) struct FileMetadataInternerState {
    files: [HashInterner<ScopedMetadata<FileMeta>>; SHARD_COUNT],
    expressions: [HashInterner<ScopedMetadata<PartitionExpr>>; SHARD_COUNT],
    hasher: RandomState,
}

/// Shared engine-local metadata interner.
pub type FileMetadataInternerRef = Arc<FileMetadataInterner>;

impl Default for FileMetadataInterner {
    fn default() -> Self {
        let inner = Arc::new(FileMetadataInternerState::default());
        FILE_METADATA_COLLECTOR.register(&inner);
        Self { inner }
    }
}

impl FileMetadataInternerState {
    /// Counts registered values, not runtime views or allocations outside this interner.
    /// Each shard exposes its entry count without traversing entries.
    pub(crate) fn entry_counts(&self) -> (usize, usize) {
        (
            self.files.iter().map(HashInterner::len).sum(),
            self.expressions.iter().map(HashInterner::len).sum(),
        )
    }
}

impl Debug for FileMetadataInterner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileMetadataInterner")
            .finish_non_exhaustive()
    }
}

impl FileMetadataInterner {
    /// Creates an interner with automatic reclamation and scrape-time metrics.
    pub fn new() -> FileMetadataInternerRef {
        Arc::new(Self::default())
    }

    /// Normalizes exclusively owned metadata before publishing it to readers.
    /// Shared inputs remain immutable: expression deduplication must not deep-clone
    /// indexes merely to replace an equivalent expression.
    pub(crate) fn intern_in_place(&self, meta: &mut FileMetaRef) {
        if let Some(expr) = &meta.partition_expr {
            let table_id = meta.region_id.table_id();
            let shard = self.inner.hasher.hash_one(table_id) as usize % SHARD_COUNT;
            let shared = MetadataRef(MetadataRefInner::Interned(
                self.inner.expressions[shard].intern_sized(ScopedMetadata {
                    table_id,
                    value: expr.as_arc().clone(),
                }),
            ));
            if let Some(owned) = meta.get_mut() {
                owned.partition_expr = Some(shared);
            }
        }
        let key = RegionFileId::new(meta.region_id, meta.file_id);
        let shard = self.inner.hasher.hash_one(key) as usize % SHARD_COUNT;
        *meta = MetadataRef(MetadataRefInner::Interned(
            self.inner.files[shard].intern_sized(ScopedMetadata {
                table_id: meta.region_id.table_id(),
                value: meta.as_arc().clone(),
            }),
        ));
    }

    /// Shares only fully equal metadata; file identity alone is insufficient.
    pub(crate) fn intern(&self, mut meta: FileMetaRef) -> FileMetaRef {
        self.intern_in_place(&mut meta);
        meta
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::sst::file::FileMeta;

    #[test]
    fn test_retirement_after_delayed_reader_release() {
        let interner = FileMetadataInterner::new();
        let values: Vec<_> = (0..4096)
            .map(|_| {
                interner.intern(
                    FileMeta {
                        region_id: RegionId::new(1, 1),
                        file_id: FileId::random(),
                        partition_expr: Some(
                            partition::expr::col("tag")
                                .lt(datatypes::value::Value::UInt32(10))
                                .into(),
                        ),
                        ..Default::default()
                    }
                    .into(),
                )
            })
            .collect();
        let reader = values[0].clone();
        drop(values);
        assert_eq!(
            interner
                .inner
                .files
                .iter()
                .map(HashInterner::len)
                .sum::<usize>(),
            1
        );
        assert_eq!(
            interner
                .inner
                .expressions
                .iter()
                .map(HashInterner::len)
                .sum::<usize>(),
            1
        );
        let recovered = interner.intern(reader.as_ref().clone().into());
        assert!(MetadataRef::ptr_eq(&reader, &recovered));
        drop(recovered);
        drop(reader);
        assert!(interner.inner.files.iter().all(HashInterner::is_empty));
        assert!(
            interner
                .inner
                .expressions
                .iter()
                .all(HashInterner::is_empty)
        );
    }

    #[test]
    fn test_historical_expression_types_share_only_equal_metadata() {
        use datatypes::value::Value;
        use partition::expr::col;

        let interner = FileMetadataInterner::default();
        let mut retained = Vec::new();
        let values = [
            Value::Int32(10),
            Value::Int64(10),
            Value::UInt32(10),
            serde_json::from_value(serde_json::json!({
                "Decimal128": {"value": 10, "precision": 10, "scale": 0}
            }))
            .unwrap(),
            serde_json::from_value(serde_json::json!({
                "Decimal128": {"value": 10, "precision": 10, "scale": 1}
            }))
            .unwrap(),
        ];
        let expected_count = values.len();
        for value in values {
            let meta = FileMeta {
                region_id: RegionId::new(1, 1),
                partition_expr: Some(col("tag").lt(value).into()),
                ..Default::default()
            };
            // Recovery creates independent allocations for historical metadata.
            let encoded = serde_json::to_vec(&meta).unwrap();
            let first = interner.intern(serde_json::from_slice(&encoded).unwrap());
            let recovered = interner.intern(serde_json::from_slice(&encoded).unwrap());
            assert!(MetadataRef::ptr_eq(&first, &recovered));
            for old in &retained {
                assert!(!MetadataRef::ptr_eq(old, &first));
                assert_eq!(
                    interner.inner.hasher.hash_one(old),
                    interner.inner.hasher.hash_one(&first)
                );
                assert!(!MetadataRef::ptr_eq(
                    old.partition_expr.as_ref().unwrap(),
                    first.partition_expr.as_ref().unwrap(),
                ));
            }
            retained.push(first);
        }
        assert_eq!(
            (expected_count, expected_count),
            interner.inner.entry_counts()
        );
        drop(retained);
        assert_eq!((0, 0), interner.inner.entry_counts());
    }

    #[test]
    fn test_metadata_views_and_recovered_copies() {
        let pool = FileMetadataInterner::default();
        let original = MetadataRef::from(FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: FileId::random(),
            partition_expr: PartitionExpr::from_json_str(
                r#"{"Expr":{"lhs":{"Column":"tag"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#,
            )
            .unwrap()
            .map(MetadataRef::from),
            ..Default::default()
        });
        let encoded = serde_json::to_vec(original.as_ref()).unwrap();
        let first = pool.intern(original);
        let recovered = pool.intern(serde_json::from_slice(&encoded).unwrap());
        assert!(MetadataRef::ptr_eq(&first, &recovered));
        assert_eq!(encoded, serde_json::to_vec(&recovered).unwrap());

        let mut changed = first.as_ref().clone();
        changed.index_version += 1;
        let changed = pool.intern(MetadataRef::from(changed));
        assert!(!MetadataRef::ptr_eq(&first, &changed));
        assert_eq!(first.index_version, 0);
        assert_eq!(changed.index_version, 1);

        let mut other_file = first.as_ref().clone();
        other_file.file_id = FileId::random();
        other_file.region_id = RegionId::new(1, 2);
        let other_file =
            pool.intern(serde_json::from_slice(&serde_json::to_vec(&other_file).unwrap()).unwrap());
        assert!(!MetadataRef::ptr_eq(&first, &other_file));
        assert!(MetadataRef::ptr_eq(
            first.partition_expr.as_ref().unwrap(),
            other_file.partition_expr.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_table_pool_keeps_historical_expressions_distinct() {
        let pool = FileMetadataInterner::default();
        let old_expr = partition::expr::col("tag").lt(datatypes::value::Value::UInt32(10));
        let new_expr = partition::expr::col("tag").lt(datatypes::value::Value::UInt32(5));
        let old = pool.intern(MetadataRef::from(FileMeta {
            region_id: RegionId::new(1, 1),
            partition_expr: Some(MetadataRef::from(old_expr.clone())),
            ..Default::default()
        }));
        for (region_id, expr) in [
            (RegionId::new(1, 2), new_expr.clone()),
            (RegionId::new(2, 1), old_expr.clone()),
        ] {
            let other = pool.intern(MetadataRef::from(FileMeta {
                region_id,
                partition_expr: Some(MetadataRef::from(expr.clone())),
                ..Default::default()
            }));
            assert!(!MetadataRef::ptr_eq(
                old.partition_expr.as_ref().unwrap(),
                other.partition_expr.as_ref().unwrap(),
            ));
            assert_eq!(Some(&expr), other.partition_expr.as_deref());
        }
        assert_eq!(Some(&old_expr), old.partition_expr.as_deref());
    }

    #[test]
    fn test_same_index_different_contents_remain_shared() {
        let pool = FileMetadataInterner::default();
        let first = pool.intern(MetadataRef::from(FileMeta::default()));
        let second = pool.intern(MetadataRef::from(FileMeta {
            file_size: 123,
            ..first.as_ref().clone()
        }));
        assert!(!MetadataRef::ptr_eq(&first, &second));
        assert!(MetadataRef::ptr_eq(
            &first,
            &pool.intern(MetadataRef::from(first.as_ref().clone()))
        ));
        assert!(MetadataRef::ptr_eq(
            &second,
            &pool.intern(MetadataRef::from(second.as_ref().clone()))
        ));
    }
    #[test]
    fn test_concurrent_publication_and_retirement() {
        let interner = FileMetadataInterner::new();
        let collector = crate::metrics::file_metadata::FileMetadataCollector::default();
        collector.register(&interner.inner);
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    let interner = &interner;
                    let barrier = &barrier;
                    let collector = &collector;
                    scope.spawn(move || {
                        barrier.wait();
                        for i in 0..1000 {
                            if i % 64 == 0 {
                                use prometheus::core::Collector;
                                assert_eq!(collector.collect().len(), 1);
                            }
                            let meta = FileMeta {
                                region_id: RegionId::new(1, 1),
                                file_size: i % 16,
                                partition_expr: Some(
                                    partition::expr::col("tag")
                                        .lt(datatypes::value::Value::UInt32(10))
                                        .into(),
                                ),
                                ..Default::default()
                            };
                            let first = interner.intern(meta.clone().into());
                            let second = interner.intern(meta.into());
                            assert!(MetadataRef::ptr_eq(&first, &second));
                            assert!(MetadataRef::ptr_eq(
                                first.partition_expr.as_ref().unwrap(),
                                second.partition_expr.as_ref().unwrap()
                            ));
                        }
                    })
                })
                .collect();
            for worker in workers {
                worker.join().unwrap();
            }
        });
        assert!(interner.inner.files.iter().all(HashInterner::is_empty));
        assert!(
            interner
                .inner
                .expressions
                .iter()
                .all(HashInterner::is_empty)
        );
    }

    #[test]
    fn test_engine_isolation_and_reader_outlives_interner() {
        let first = FileMetadataInterner::new();
        let second = FileMetadataInterner::new();
        let left = first.intern(FileMeta::default().into());
        let right = second.intern(FileMeta::default().into());
        assert!(!MetadataRef::ptr_eq(&left, &right));
        drop(first);
        assert_eq!(*left, *right);
    }
    #[test]
    fn test_scrape_counts_distinct_values_and_releases_sources() {
        use std::collections::BTreeMap;

        use crate::metrics::file_metadata::FileMetadataCollector;

        let collector = FileMetadataCollector::default();
        let registry = prometheus::Registry::new();
        registry.register(Box::new(collector.clone())).unwrap();
        let counts = || {
            let families = registry.gather();
            assert_eq!(families.len(), 1);
            assert_eq!(
                families[0].name(),
                "greptime_mito_sst_metadata_interner_entries"
            );
            families[0]
                .get_metric()
                .iter()
                .map(|metric| {
                    assert_eq!(metric.get_label().len(), 1);
                    let label = &metric.get_label()[0];
                    assert_eq!(label.name(), "type");
                    (
                        label.value().to_string(),
                        metric.get_gauge().value() as usize,
                    )
                })
                .collect::<BTreeMap<_, _>>()
        };
        let expected = |files, expressions| {
            BTreeMap::from([
                ("file".to_string(), files),
                ("partition_expr".to_string(), expressions),
            ])
        };
        assert_eq!(counts(), expected(0, 0));
        let first = FileMetadataInterner::default();
        let second = FileMetadataInterner::new();
        collector.register(&first.inner);
        collector.register(&second.inner);
        let make_meta = || FileMeta {
            region_id: RegionId::new(1, 1),
            partition_expr: Some(
                partition::expr::col("tag")
                    .lt(datatypes::value::Value::UInt32(10))
                    .into(),
            ),
            ..Default::default()
        };
        let value = first.intern(make_meta().into());
        let view = first.intern(make_meta().into());
        let other_engine = second.intern(make_meta().into());
        let changed = first.intern(
            FileMeta {
                file_size: 1,
                ..make_meta()
            }
            .into(),
        );
        // Views deduplicate, content versions and engine scopes do not.
        assert_eq!(counts(), expected(3, 2));
        drop(value);
        drop(changed);
        drop(other_engine);
        assert_eq!(counts(), expected(1, 1));
        drop(view);
        assert_eq!(counts(), expected(0, 0));

        let reader = first.intern(make_meta().into());
        let weak = Arc::downgrade(&first.inner);
        drop(first);
        drop(second);
        assert!(weak.upgrade().is_none());
        // Counts describe registered entries in live interners, not detached readers.
        assert_eq!(counts(), expected(0, 0));
        assert_eq!(reader.region_id, RegionId::new(1, 1));
    }
}
