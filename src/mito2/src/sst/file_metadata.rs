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
use std::hash::BuildHasher;
use std::ops::Deref;
use std::sync::Arc;

use intern_arc::{InternedOrd, OrdInterner};
use partition::expr::PartitionExpr;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use store_api::storage::TableId;

use crate::sst::file::{FileMeta, FileMetaRef, RegionFileId};

const SHARD_COUNT: usize = 32;

/// Immutable metadata, either freshly decoded or registered with an engine interner.
/// Keeping the decoded allocation avoids copying index vectors during recovery.
#[derive(Clone)]
pub struct MetadataRef<T: Ord>(MetadataRefInner<T>);

#[derive(Clone)]
enum MetadataRefInner<T: Ord> {
    Owned(Arc<T>),
    Interned(InternedOrd<ScopedMetadata<T>>),
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct ScopedMetadata<T: Ord> {
    table_id: TableId,
    value: Arc<T>,
}

impl<T: Ord> MetadataRef<T> {
    fn as_arc(&self) -> &Arc<T> {
        match &self.0 {
            MetadataRefInner::Owned(value) => value,
            MetadataRefInner::Interned(value) => &value.value,
        }
    }

    fn get_mut(&mut self) -> Option<&mut T> {
        match &mut self.0 {
            MetadataRefInner::Owned(value) => Arc::get_mut(value),
            // Mutating a registered value would invalidate the tree ordering.
            MetadataRefInner::Interned(_) => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn ptr_eq(left: &Self, right: &Self) -> bool {
        Arc::ptr_eq(left.as_arc(), right.as_arc())
    }
}

impl<T: Ord> From<T> for MetadataRef<T> {
    fn from(value: T) -> Self {
        Self::from(Arc::new(value))
    }
}

impl<T: Ord> From<Arc<T>> for MetadataRef<T> {
    fn from(value: Arc<T>) -> Self {
        Self(MetadataRefInner::Owned(value))
    }
}

impl<T: Ord + Default> Default for MetadataRef<T> {
    fn default() -> Self {
        T::default().into()
    }
}

impl<T: Ord> Deref for MetadataRef<T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.as_arc()
    }
}

impl<T: Ord> AsRef<T> for MetadataRef<T> {
    fn as_ref(&self) -> &T {
        self
    }
}

impl<T: Ord + Debug> Debug for MetadataRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Debug::fmt(self.as_ref(), f)
    }
}

impl<T: Ord + Display> Display for MetadataRef<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self.as_ref(), f)
    }
}

impl<T: Ord> PartialEq for MetadataRef<T> {
    fn eq(&self, other: &Self) -> bool {
        self.as_arc() == other.as_arc()
    }
}

impl<T: Ord> Eq for MetadataRef<T> {}

impl<T: Ord> PartialOrd for MetadataRef<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: Ord> Ord for MetadataRef<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_arc().cmp(other.as_arc())
    }
}

impl<T: Ord + Serialize> Serialize for MetadataRef<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.as_ref().serialize(serializer)
    }
}

impl<'de, T: Ord + Deserialize<'de>> Deserialize<'de> for MetadataRef<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Decoding must not register values in the crate's process-global pool.
        T::deserialize(deserializer).map(Self::from)
    }
}

/// Engine-local sharing. File and expression operations never hold nested locks.
/// The final registered reference removes its entry and releases unused tree nodes.
#[derive(Default)]
pub(crate) struct FileMetadataInterner {
    files: [OrdInterner<ScopedMetadata<FileMeta>>; SHARD_COUNT],
    expressions: [OrdInterner<ScopedMetadata<PartitionExpr>>; SHARD_COUNT],
    hasher: RandomState,
}

pub(crate) type FileMetadataInternerRef = Arc<FileMetadataInterner>;

impl Debug for FileMetadataInterner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileMetadataInterner")
            .finish_non_exhaustive()
    }
}

impl FileMetadataInterner {
    pub(crate) fn new() -> FileMetadataInternerRef {
        Arc::new(Self::default())
    }

    /// Normalizes exclusively owned metadata before publishing it to readers.
    /// Shared inputs remain immutable: expression deduplication must not deep-clone
    /// indexes merely to replace an equivalent expression.
    pub(crate) fn intern_in_place(&self, meta: &mut FileMetaRef) {
        if let Some(expr) = &meta.partition_expr {
            let table_id = meta.region_id.table_id();
            let shard = self.hasher.hash_one(table_id) as usize % SHARD_COUNT;
            let shared = MetadataRef(MetadataRefInner::Interned(
                self.expressions[shard].intern_sized(ScopedMetadata {
                    table_id,
                    value: expr.as_arc().clone(),
                }),
            ));
            if let Some(owned) = meta.get_mut() {
                owned.partition_expr = Some(shared);
            }
        }
        let key = RegionFileId::new(meta.region_id, meta.file_id);
        let shard = self.hasher.hash_one(key) as usize % SHARD_COUNT;
        *meta = MetadataRef(MetadataRefInner::Interned(self.files[shard].intern_sized(
            ScopedMetadata {
                table_id: meta.region_id.table_id(),
                value: meta.as_arc().clone(),
            },
        )));
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
            interner.files.iter().map(OrdInterner::len).sum::<usize>(),
            1
        );
        assert_eq!(
            interner
                .expressions
                .iter()
                .map(OrdInterner::len)
                .sum::<usize>(),
            1
        );
        let recovered = interner.intern(reader.as_ref().clone().into());
        assert!(MetadataRef::ptr_eq(&reader, &recovered));
        drop(recovered);
        drop(reader);
        assert!(interner.files.iter().all(OrdInterner::is_empty));
        assert!(interner.expressions.iter().all(OrdInterner::is_empty));
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
        let barrier = std::sync::Barrier::new(8);
        std::thread::scope(|scope| {
            let workers: Vec<_> = (0..8)
                .map(|_| {
                    let interner = &interner;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        for i in 0..1000 {
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
        assert!(interner.files.iter().all(OrdInterner::is_empty));
        assert!(interner.expressions.iter().all(OrdInterner::is_empty));
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
}
