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

use std::fmt;
use std::sync::Arc;

use common_telemetry::error;
use store_api::region_request::PathType;
use store_api::storage::FileId;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileMeta, delete_files};
use crate::sst::file_ref::FileReferenceManagerRef;
use crate::sst::range_index::RangeIndexDeleter;

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync + fmt::Debug {
    /// Purges a retired SST and its indexes after all metadata views release their readers.
    /// Each metadata view has already released its reader reference.
    fn purge_file(&self, file_meta: FileMeta);

    /// Retire cached scan results immediately, without waiting for physical deletion.
    fn invalidate_file(&self, _: &FileMeta) {}

    /// Returns the object-store reference manager used by SST metadata views.
    /// Local and no-op purgers do not track GC references.
    fn file_ref_manager(&self) -> Option<FileReferenceManagerRef> {
        None
    }
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

/// A no-op file purger can be used in combination with reading SST files outside of this region.
#[derive(Debug)]
pub struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn purge_file(&self, _file_meta: FileMeta) {}
}

/// Purger that purges file for current region.
pub struct LocalFilePurger {
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    range_index_deleter: Option<RangeIndexDeleter>,
}

impl fmt::Debug for LocalFilePurger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFilePurger")
            .field("sst_layer", &self.sst_layer)
            .finish()
    }
}

#[cfg(not(any(debug_assertions, test)))]
/// Whether to enable GC for the file purger.
pub fn should_enable_gc(global_gc_enabled: bool, object_store_scheme: &'static str) -> bool {
    global_gc_enabled && object_store_scheme != object_store::services::FS_SCHEME
}

#[cfg(any(debug_assertions, test))]
/// Debug builds and unit tests use Fs to exercise object-store GC, including
/// unit tests compiled with the release profile.
pub fn should_enable_gc(global_gc_enabled: bool, _object_store_scheme: &'static str) -> bool {
    global_gc_enabled
}

/// Creates a file purger based on the storage type of the access layer.
/// Should be use in combination with Gc Worker.
///
/// If the storage is local file system, a `LocalFilePurger` is created, which deletes
/// the files from both the storage and the cache.
///
/// If the storage is an object store, an `ObjectStoreFilePurger` is created, which
/// only manages SST file references. Companion range indexes are deleted directly in either
/// mode on final handle release when the SST is marked deleted and a deleter is provided.
///
pub fn create_file_purger(
    gc_enabled: bool,
    path_type: PathType,
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
    range_index_deleter: Option<RangeIndexDeleter>,
) -> FilePurgerRef {
    // Only enable GC for:
    // - object store based storage
    // - data or bare path type (metadata region doesn't need to be GCed)
    if should_enable_gc(gc_enabled, sst_layer.object_store().info().scheme())
        && matches!(path_type, PathType::Data | PathType::Bare)
    {
        Arc::new(ObjectStoreFilePurger {
            cache_manager,
            file_ref_manager,
            scheduler,
            range_index_deleter,
        })
    } else {
        Arc::new(
            LocalFilePurger::new(scheduler, sst_layer, cache_manager)
                .with_range_index_deleter(range_index_deleter),
        )
    }
}

/// Creates a local file purger that deletes files from both the storage and the cache.
pub fn create_local_file_purger(
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    _file_ref_manager: FileReferenceManagerRef,
    range_index_deleter: Option<RangeIndexDeleter>,
) -> FilePurgerRef {
    Arc::new(
        LocalFilePurger::new(scheduler, sst_layer, cache_manager)
            .with_range_index_deleter(range_index_deleter),
    )
}

impl LocalFilePurger {
    /// Creates a new purger.
    pub fn new(
        scheduler: SchedulerRef,
        sst_layer: AccessLayerRef,
        cache_manager: Option<CacheManagerRef>,
    ) -> Self {
        Self {
            scheduler,
            sst_layer,
            cache_manager,
            range_index_deleter: None,
        }
    }

    /// Attaches deletion of companion range indexes.
    pub fn with_range_index_deleter(mut self, deleter: Option<RangeIndexDeleter>) -> Self {
        self.range_index_deleter = deleter;
        self
    }

    /// Stop the scheduler of the file purger.
    pub async fn stop_scheduler(&self) -> Result<()> {
        self.scheduler.stop(true).await
    }

    /// Deletes the file(and it's index, if any) from cache and storage.
    fn delete_file(&self, file_meta: FileMeta) {
        let sst_layer = self.sst_layer.clone();
        let cache_manager = self.cache_manager.clone();
        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            if let Err(e) = delete_files(
                file_meta.region_id,
                &[(file_meta.file_id, file_meta.index_id().version)],
                file_meta.exists_index(),
                &sst_layer,
                &cache_manager,
            )
            .await
            {
                error!(e; "Failed to delete file {:?} from storage", file_meta);
            }
        })) {
            error!(e; "Failed to schedule the file purge request");
        }
    }
}

impl FilePurger for LocalFilePurger {
    fn invalidate_file(&self, file_meta: &FileMeta) {
        if let Some(cache) = &self.cache_manager {
            cache.invalidate_range_results(file_meta.file_id);
        }
    }

    fn purge_file(&self, file_meta: FileMeta) {
        schedule_range_index_deletion(
            &self.scheduler,
            self.range_index_deleter.as_ref(),
            file_meta.file_id,
        );
        self.delete_file(file_meta);
    }
}

pub struct ObjectStoreFilePurger {
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
    scheduler: SchedulerRef,
    range_index_deleter: Option<RangeIndexDeleter>,
}

impl fmt::Debug for ObjectStoreFilePurger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ObjectStoreFilePurger")
            .field("file_ref_manager", &self.file_ref_manager)
            .field("range_index_deleter", &self.range_index_deleter)
            .finish_non_exhaustive()
    }
}

/// Range indexes are deleted directly even when SST deletion is delegated to GC.
fn schedule_range_index_deletion(
    scheduler: &SchedulerRef,
    deleter: Option<&RangeIndexDeleter>,
    file_id: FileId,
) {
    let Some(deleter) = deleter.cloned() else {
        return;
    };
    if let Err(error) = scheduler.schedule(Box::pin(async move {
        if let Err(error) = deleter.delete(file_id).await {
            error!(error; "Failed to delete range index, file_id: {file_id}");
        }
    })) {
        error!(error; "Failed to schedule range-index deletion, file_id: {file_id}");
    }
}

impl FilePurger for ObjectStoreFilePurger {
    fn invalidate_file(&self, file_meta: &FileMeta) {
        if let Some(cache) = &self.cache_manager {
            cache.invalidate_range_results(file_meta.file_id);
        }
    }

    fn purge_file(&self, file_meta: FileMeta) {
        schedule_range_index_deletion(
            &self.scheduler,
            self.range_index_deleter.as_ref(),
            file_meta.file_id,
        );
    }

    fn file_ref_manager(&self) -> Option<FileReferenceManagerRef> {
        Some(self.file_ref_manager.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;
    use std::sync::Barrier;
    use std::thread;

    use common_test_util::temp_dir::{TempDir, create_temp_dir};
    use object_store::ObjectStore;
    use object_store::services::Fs;
    use smallvec::SmallVec;
    use store_api::region_request::PathType;
    use store_api::storage::{FileId, RegionId};

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::schedule::scheduler::{LocalScheduler, Scheduler};
    use crate::sst::file::{
        ColumnIndexMetadata, FileHandle, FileMeta, FileTimeRange, IndexType, RegionFileId,
        RegionIndexId,
    };
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::location;

    struct PurgeTestEnv {
        layer: AccessLayerRef,
        scheduler: Arc<LocalScheduler>,
        file_id: RegionFileId,
        _dir: TempDir,
    }

    impl PurgeTestEnv {
        async fn new() -> Self {
            common_telemetry::init_default_ut_logging();
            let dir = create_temp_dir("file-purge");
            let index_aux_path = dir.path().join("index_aux");
            let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
                .await
                .unwrap();
            let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
                .await
                .unwrap();
            let store =
                ObjectStore::new(Fs::default().root(&dir.path().display().to_string())).unwrap();
            let env = Self {
                layer: Arc::new(AccessLayer::new(
                    "table1",
                    PathType::Bare,
                    store,
                    puffin_mgr,
                    intm_mgr,
                )),
                scheduler: Arc::new(LocalScheduler::new(1)),
                file_id: RegionFileId::new(RegionId::new(0, 0), FileId::random()),
                _dir: dir,
            };
            env.layer
                .object_store()
                .write(&env.sst_path(), vec![0; 4096])
                .await
                .unwrap();
            env
        }

        fn sst_path(&self) -> String {
            location::sst_file_path(self.layer.table_dir(), self.file_id, self.layer.path_type())
        }

        fn index_path(&self, version: u64) -> String {
            location::index_file_path(
                self.layer.table_dir(),
                RegionIndexId::new(self.file_id, version),
                self.layer.path_type(),
            )
        }

        async fn wait_for_purges(&self) {
            // The single worker finishes all earlier purge jobs before this barrier.
            let (tx, rx) = tokio::sync::oneshot::channel();
            self.scheduler
                .schedule(Box::pin(async move {
                    tx.send(()).unwrap();
                }))
                .unwrap();
            rx.await.unwrap();
        }
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_concurrent_index_view_release_keeps_live_files_and_purges_retired_files(
        #[values(false, true)] retired: bool,
    ) {
        let env = PurgeTestEnv::new().await;
        let store = env.layer.object_store();
        let purger = Arc::new(LocalFilePurger::new(
            env.scheduler.clone(),
            env.layer.clone(),
            None,
        ));
        let meta = FileMeta {
            region_id: env.file_id.region_id(),
            file_id: env.file_id.file_id(),
            ..Default::default()
        };
        let old_reader = FileHandle::new(meta.clone(), purger);
        let mut readers = vec![old_reader];
        for index_version in 1..=2 {
            store
                .write(&env.index_path(index_version), "index")
                .await
                .unwrap();
            readers.push(readers.last().unwrap().with_updated_index(FileMeta {
                index_version,
                available_indexes: smallvec::smallvec![IndexType::InvertedIndex],
                ..meta.clone()
            }));
        }
        if retired {
            readers.last().unwrap().mark_deleted();
        }
        // Dropping one view must not remove files still needed by the other views.
        drop(readers.remove(1));
        env.wait_for_purges().await;
        assert!(store.exists(&env.sst_path()).await.unwrap());
        for version in 1..=2 {
            assert!(store.exists(&env.index_path(version)).await.unwrap());
        }

        // Regression coverage for final releases on different threads. The start barrier
        // encourages overlap; it does not exhaustively control destructor interleavings.
        let barrier = Barrier::new(readers.len());
        thread::scope(|scope| {
            for reader in readers {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    drop(reader);
                });
            }
        });
        env.scheduler.stop(true).await.unwrap();
        assert_eq!(store.exists(&env.sst_path()).await.unwrap(), !retired);
        for version in 1..=2 {
            assert_eq!(
                store.exists(&env.index_path(version)).await.unwrap(),
                !retired
            );
        }
    }

    #[rstest::rstest]
    #[case(false)]
    #[case(true)]
    #[tokio::test]
    async fn test_range_index_purge_on_handle_release(
        #[case] gc_enabled: bool,
        #[values(false, true)] is_delete: bool,
        #[values(false, true)] update_index: bool,
    ) {
        let env = PurgeTestEnv::new().await;
        let object_store = env.layer.object_store();
        let path = env.sst_path();
        let sst_file_id = env.file_id;
        let scheduler = &env.scheduler;

        let index_store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let owner = RegionId::new(9, 1);
        let index_path = crate::sst::range_index::range_index_path(owner, sst_file_id.file_id());
        index_store.write(&index_path, "range index").await.unwrap();
        let cache = Arc::new(
            crate::cache::CacheManager::builder()
                .range_result_cache_size(1024 * 1024)
                .build(),
        );
        let cache_key = crate::cache::test_util::range_cache_key(&[sst_file_id.file_id()]);
        cache.put_range_result(
            cache_key.clone(),
            Arc::new(crate::read::range_cache::RangeScanCacheValue::new(
                vec![],
                0,
            )),
        );
        let file_purger = create_file_purger(
            gc_enabled,
            PathType::Bare,
            scheduler.clone(),
            env.layer.clone(),
            Some(cache.clone()),
            Arc::new(crate::sst::file_ref::FileReferenceManager::new(None)),
            Some(RangeIndexDeleter::new(index_store.clone(), owner)),
        );

        {
            let meta = FileMeta {
                region_id: sst_file_id.region_id(),
                file_id: sst_file_id.file_id(),
                file_size: 4096,
                max_row_group_uncompressed_size: 4096,
                ..Default::default()
            };
            let mut version = crate::sst::version::SstVersion::new(
                crate::test_util::memtable_util::metadata_for_test(),
            );
            version.add_files(file_purger, [meta].into_iter());
            let reader = version.levels()[0].files[&sst_file_id.file_id()].clone();
            if update_index {
                for index_version in 1..=2 {
                    let mut meta = reader.meta_ref().clone();
                    meta.index_version = index_version;
                    meta.available_indexes.push(IndexType::InvertedIndex);
                    let path = env.index_path(index_version);
                    object_store.write(&path, "index").await.unwrap();
                    version.add_files(reader.file_purger(), [meta].into_iter());
                }
            }
            if is_delete {
                version.mark_all_deleted();
            }
            // Invalidate before any old index-version reader releases the SST.
            assert_eq!(cache.get_range_result(&cache_key).is_none(), is_delete);
            drop(version);
            env.wait_for_purges().await;
            assert!(object_store.exists(&path).await.unwrap());
            assert!(index_store.exists(&index_path).await.unwrap());
            if update_index {
                for index_version in 1..=2 {
                    let path = env.index_path(index_version);
                    assert!(object_store.exists(&path).await.unwrap());
                }
            }
            drop(reader);
        }

        scheduler.stop(true).await.unwrap();

        assert_eq!(
            object_store.exists(&path).await.unwrap(),
            gc_enabled || !is_delete
        );
        assert_eq!(index_store.exists(&index_path).await.unwrap(), !is_delete);
        if update_index {
            for index_version in 1..=2 {
                let path = env.index_path(index_version);
                assert_eq!(
                    object_store.exists(&path).await.unwrap(),
                    gc_enabled || !is_delete
                );
            }
        }
    }

    #[tokio::test]
    async fn test_file_purge_with_index() {
        let env = PurgeTestEnv::new().await;
        let object_store = env.layer.object_store();
        let sst_file_id = env.file_id;
        let path = env.sst_path();
        let index_path = env.index_path(0);
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let scheduler = &env.scheduler;
        let file_purger = Arc::new(LocalFilePurger::new(
            scheduler.clone(),
            env.layer.clone(),
            None,
        ));

        {
            let handle = FileHandle::new(
                FileMeta {
                    region_id: sst_file_id.region_id(),
                    file_id: sst_file_id.file_id(),
                    time_range: FileTimeRange::default(),
                    level: 0,
                    file_size: 4096,
                    max_row_group_uncompressed_size: 4096,
                    available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
                    indexes: vec![ColumnIndexMetadata {
                        column_id: 0,
                        created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
                        ..Default::default()
                    }],
                    index_file_size: 4096,
                    index_version: 0,
                    num_rows: 1024,
                    num_row_groups: 1,
                    sequence: NonZeroU64::new(4096),
                    partition_expr: None,
                    num_series: 0,
                    ..Default::default()
                },
                file_purger,
            );
            // mark file as deleted and drop the handle, we expect the sst file and the index file are deleted.
            handle.mark_deleted();
        }

        scheduler.stop(true).await.unwrap();

        assert!(!object_store.exists(&path).await.unwrap());
        assert!(!object_store.exists(&index_path).await.unwrap());
    }
}
