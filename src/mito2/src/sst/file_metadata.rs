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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use partition::expr::PartitionExpr;
use smallvec::SmallVec;
use store_api::storage::TableId;

use crate::sst::file::{FileMeta, FileMetaRef, RegionFileId};

/// Engine-local pool. Weak references do not keep metadata payloads alive.
#[derive(Debug, Default)]
pub(crate) struct FileMetadataPool {
    entries: Mutex<MetadataEntries>,
}

pub(crate) type FileMetadataPoolRef = Arc<FileMetadataPool>;

impl FileMetadataPool {
    /// Evicts a metadata generation after its last registered reader leaves.
    /// Manifest-only copies may remain alive; eviction affects sharing, not ownership.
    pub(crate) fn remove_file(&self, meta: &FileMeta) {
        let mut entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        let key = RegionFileId::new(meta.region_id, meta.file_id);
        if let std::collections::hash_map::Entry::Occupied(mut entry) = entries.files.entry(key) {
            entry.get_mut().retain(|version| {
                version
                    .upgrade()
                    .is_some_and(|version| version.index_version != meta.index_version)
            });
            if entry.get().is_empty() {
                entry.remove();
            }
        }
        if entries.files.is_empty() {
            entries.expressions.clear();
            entries.expressions.shrink_to_fit();
        }
        // Shrink geometrically rather than rehashing on every file removal.
        if entries.files.capacity() > entries.files.len().saturating_mul(4).max(1024) {
            entries.files.shrink_to_fit();
            entries.next_prune = entries.files.len().saturating_mul(2).max(1024);
        }
    }

    /// Shares only fully equal metadata; file identity alone is insufficient.
    pub(crate) fn intern(&self, mut meta: FileMetaRef) -> FileMetaRef {
        let mut entries = self.entries.lock().unwrap_or_else(|err| err.into_inner());
        entries.prune_if_needed();
        let key = RegionFileId::new(meta.region_id, meta.file_id);
        if let Some(versions) = entries.files.get_mut(&key) {
            versions.retain(|version| version.strong_count() > 0);
            if let Some(existing) = versions
                .iter()
                .filter_map(Weak::upgrade)
                .find(|existing| Arc::ptr_eq(existing, &meta) || **existing == *meta)
            {
                return existing;
            }
        }
        if let Some(expr) = &meta.partition_expr {
            let expressions = entries
                .expressions
                .entry(meta.region_id.table_id())
                .or_default();
            expressions.retain(|expr| expr.strong_count() > 0);
            if let Some(existing) = expressions
                .iter()
                .filter_map(Weak::upgrade)
                .find(|existing| Arc::ptr_eq(existing, expr) || **existing == **expr)
            {
                if !Arc::ptr_eq(&existing, expr) {
                    Arc::make_mut(&mut meta).partition_expr = Some(existing);
                }
            } else {
                expressions.push(Arc::downgrade(expr));
            }
        }
        entries
            .files
            .entry(key)
            .or_default()
            .push(Arc::downgrade(&meta));
        meta
    }
}

#[derive(Debug, Default)]
struct MetadataEntries {
    files: HashMap<RegionFileId, SmallVec<[Weak<FileMeta>; 1]>>,
    expressions: HashMap<TableId, Vec<Weak<PartitionExpr>>>,
    next_prune: usize,
}

impl MetadataEntries {
    /// Geometric thresholds amortize scans during large manifest loads.
    fn prune_if_needed(&mut self) {
        if self.files.len() < self.next_prune.max(1024) {
            return;
        }
        self.files.retain(|_, versions| {
            versions.retain(|version| version.strong_count() > 0);
            !versions.is_empty()
        });
        self.expressions.retain(|_, expressions| {
            expressions.retain(|expr| expr.strong_count() > 0);
            !expressions.is_empty()
        });
        self.next_prune = self.files.len().saturating_mul(2).max(1024);
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::{FileId, RegionId};

    use super::*;

    #[test]
    fn test_metadata_views_and_recovered_copies() {
        let pool = FileMetadataPool::default();
        let original = Arc::new(FileMeta {
            region_id: RegionId::new(1, 1),
            file_id: FileId::random(),
            partition_expr: PartitionExpr::from_json_str(
                r#"{"Expr":{"lhs":{"Column":"tag"},"op":"GtEq","rhs":{"Value":{"UInt32":10}}}}"#,
            )
            .unwrap()
            .map(Arc::new),
            ..Default::default()
        });
        let first = pool.intern(original.clone());
        let encoded = serde_json::to_vec(&original).unwrap();
        let recovered = pool.intern(serde_json::from_slice(&encoded).unwrap());
        assert!(Arc::ptr_eq(&first, &recovered));
        assert_eq!(encoded, serde_json::to_vec(&recovered).unwrap());

        let mut changed = original.as_ref().clone();
        changed.index_version += 1;
        let changed = pool.intern(Arc::new(changed));
        assert!(!Arc::ptr_eq(&first, &changed));
        assert_eq!(first.index_version, 0);
        assert_eq!(changed.index_version, 1);

        let mut other_file = original.as_ref().clone();
        other_file.file_id = FileId::random();
        other_file.region_id = RegionId::new(1, 2);
        let other_file =
            pool.intern(serde_json::from_slice(&serde_json::to_vec(&other_file).unwrap()).unwrap());
        assert!(!Arc::ptr_eq(&first, &other_file));
        assert!(Arc::ptr_eq(
            first.partition_expr.as_ref().unwrap(),
            other_file.partition_expr.as_ref().unwrap()
        ));
    }

    #[test]
    fn test_table_pool_keeps_historical_expressions_distinct() {
        let pool = FileMetadataPool::default();
        let old_expr = partition::expr::col("tag").lt(datatypes::value::Value::UInt32(10));
        let new_expr = partition::expr::col("tag").lt(datatypes::value::Value::UInt32(5));
        let old = pool.intern(Arc::new(FileMeta {
            region_id: RegionId::new(1, 1),
            partition_expr: Some(Arc::new(old_expr.clone())),
            ..Default::default()
        }));
        for (region_id, expr) in [
            (RegionId::new(1, 2), new_expr.clone()),
            (RegionId::new(2, 1), old_expr.clone()),
        ] {
            let other = pool.intern(Arc::new(FileMeta {
                region_id,
                partition_expr: Some(Arc::new(expr.clone())),
                ..Default::default()
            }));
            assert!(!Arc::ptr_eq(
                old.partition_expr.as_ref().unwrap(),
                other.partition_expr.as_ref().unwrap(),
            ));
            assert_eq!(Some(&expr), other.partition_expr.as_deref());
        }
        assert_eq!(Some(&old_expr), old.partition_expr.as_deref());
    }

    #[test]
    fn test_evicting_old_generation_preserves_new_metadata() {
        let pool = FileMetadataPool::default();
        let old = pool.intern(Arc::new(FileMeta::default()));
        let current = pool.intern(Arc::new(FileMeta {
            index_version: 1,
            ..old.as_ref().clone()
        }));
        pool.remove_file(&old);
        let recovered = pool.intern(Arc::new(current.as_ref().clone()));
        assert!(Arc::ptr_eq(&current, &recovered));
        pool.remove_file(&current);
        assert!(pool.entries.lock().unwrap().files.is_empty());
        assert_eq!(old.index_version, 0);
        assert_eq!(current.index_version, 1);
    }

    #[test]
    fn test_dead_metadata_is_reclaimed() {
        let pool = FileMetadataPool::default();
        let meta = pool.intern(Arc::new(FileMeta::default()));
        let weak = Arc::downgrade(&meta);
        drop(meta);
        assert!(weak.upgrade().is_none());
        for _ in 0..2048 {
            pool.intern(Arc::new(FileMeta {
                file_id: FileId::random(),
                ..Default::default()
            }));
        }
        let entries = pool.entries.lock().unwrap();
        assert!(entries.files.len() <= 1024);
    }
}
