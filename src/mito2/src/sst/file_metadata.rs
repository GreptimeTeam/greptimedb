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

use std::sync::Arc;
use std::time::Duration;

use common_cache::WeakInterner;
use common_telemetry::error;
use partition::expr::PartitionExpr;
use store_api::storage::TableId;

use crate::sst::file::{FileMeta, FileMetaRef, RegionFileId};

/// Engine-local sharing. File and expression operations never hold nested locks.
#[derive(Debug, Default)]
pub(crate) struct FileMetadataInterner {
    files: WeakInterner<RegionFileId, FileMeta>,
    expressions: WeakInterner<TableId, PartitionExpr>,
}

pub(crate) type FileMetadataInternerRef = Arc<FileMetadataInterner>;

impl FileMetadataInterner {
    /// Starts idle maintenance without retaining the interner between passes.
    pub(crate) fn new() -> FileMetadataInternerRef {
        let interner = Arc::new(Self::default());
        let weak = Arc::downgrade(&interner);
        common_runtime::spawn_global(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(1)).await;
                let Some(interner) = weak.upgrade() else {
                    return;
                };
                // Sweeping and geometric capacity reductions run on the shared
                // blocking runtime, outside region workers and async I/O tasks.
                if let Err(err) = common_runtime::spawn_blocking_global(move || {
                    interner.files.run_pending_tasks();
                    interner.expressions.run_pending_tasks();
                })
                .await
                {
                    error!(err; "Failed to maintain SST metadata interner");
                }
            }
        });
        interner
    }

    /// Normalizes exclusively owned metadata before publishing it to readers.
    /// Shared inputs remain immutable: expression deduplication must not deep-clone
    /// indexes merely to replace an equivalent expression.
    pub(crate) fn intern_in_place(&self, meta: &mut FileMetaRef) {
        if let Some(expr) = &meta.partition_expr {
            let shared = self
                .expressions
                .intern(meta.region_id.table_id(), expr.clone());
            if let Some(owned) = Arc::get_mut(meta) {
                owned.partition_expr = Some(shared);
            }
        }
        *meta = self.files.intern(
            RegionFileId::new(meta.region_id, meta.file_id),
            meta.clone(),
        );
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

    #[tokio::test]
    async fn test_idle_maintenance_after_delayed_reader_release() {
        let manager = crate::sst::file_ref::FileReferenceManager::new(None);
        let interner = manager.file_metadata_interner.clone();
        drop(manager);
        let values: Vec<_> = (0..4096)
            .map(|_| {
                interner.intern(Arc::new(FileMeta {
                    region_id: RegionId::new(1, 1),
                    file_id: FileId::random(),
                    partition_expr: Some(Arc::new(
                        partition::expr::col("tag").lt(datatypes::value::Value::UInt32(10)),
                    )),
                    ..Default::default()
                }))
            })
            .collect();
        let reader = values[0].clone();
        drop(values);
        // Keep a reader through a maintenance pass, then release it without any
        // new intern() call. The periodic task must reclaim both indexes.
        common_runtime::spawn_blocking_global({
            let interner = interner.clone();
            move || {
                interner.files.run_pending_tasks();
                interner.expressions.run_pending_tasks();
            }
        })
        .await
        .unwrap();
        assert!(!interner.files.is_empty());
        assert!(!interner.expressions.is_empty());
        drop(reader);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !interner.files.is_empty() || !interner.expressions.is_empty() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("idle maintenance should reclaim retired metadata without new writes");
        let weak = Arc::downgrade(&interner);
        drop(interner);
        tokio::time::timeout(Duration::from_secs(5), async {
            while weak.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("maintenance must not keep its owner alive");
    }

    #[test]
    fn test_metadata_views_and_recovered_copies() {
        let pool = FileMetadataInterner::default();
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
        let pool = FileMetadataInterner::default();
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
    fn test_same_index_different_contents_remain_shared() {
        let pool = FileMetadataInterner::default();
        let first = pool.intern(Arc::new(FileMeta::default()));
        let second = pool.intern(Arc::new(FileMeta {
            file_size: 123,
            ..first.as_ref().clone()
        }));
        assert!(!Arc::ptr_eq(&first, &second));
        assert!(Arc::ptr_eq(
            &first,
            &pool.intern(Arc::new(first.as_ref().clone()))
        ));
        assert!(Arc::ptr_eq(
            &second,
            &pool.intern(Arc::new(second.as_ref().clone()))
        ));
    }
}
