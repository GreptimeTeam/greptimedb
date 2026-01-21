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

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileMeta, delete_files, delete_index};
use crate::sst::file_ref::FileReferenceManagerRef;

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync + fmt::Debug {
    /// Send a request to remove the file.
    /// If `is_delete` is true, the file will be deleted from the storage.
    /// Otherwise, only the reference will be removed.
    /// If `index_outdated` is true, the index file will be deleted regardless of `is_delete`.
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool, index_outdated: bool);

    /// Notify the purger of a new file created.
    /// This is useful for object store based storage, where we need to track the file references
    /// The default implementation is a no-op.
    fn new_file(&self, _: &FileMeta) {
        // noop
    }
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

/// A no-op file purger can be used in combination with reading SST files outside of this region.
#[derive(Debug)]
pub struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn remove_file(&self, _file_meta: FileMeta, _is_delete: bool, _index_outdated: bool) {
        // noop
    }
}

/// Purger that purges file for current region.
pub struct LocalFilePurger {
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
}

impl fmt::Debug for LocalFilePurger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFilePurger")
            .field("sst_layer", &self.sst_layer)
            .finish()
    }
}

#[cfg(not(debug_assertions))]
/// Whether to enable GC for the file purger.
pub fn should_enable_gc(
    global_gc_enabled: bool,
    object_store_scheme: object_store::Scheme,
) -> bool {
    global_gc_enabled && object_store_scheme != object_store::Scheme::Fs
}

#[cfg(debug_assertions)]
/// For debug build, we may use Fs as the object store scheme,
/// so we need to enable GC for local file system.
pub fn should_enable_gc(
    global_gc_enabled: bool,
    _object_store_scheme: object_store::Scheme,
) -> bool {
    global_gc_enabled
}

/// Creates a file purger based on the storage type of the access layer.
/// Should be use in combination with Gc Worker.
///
/// If the storage is local file system, a `LocalFilePurger` is created, which deletes
/// the files from both the storage and the cache.
///
/// If the storage is an object store, an `ObjectStoreFilePurger` is created, which
/// only manages the file references without deleting the actual files.
///
pub fn create_file_purger(
    gc_enabled: bool,
    path_type: PathType,
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
) -> FilePurgerRef {
    // Only enable GC for:
    // - object store based storage
    // - data or bare path type (metadata region doesn't need to be GCed)
    if should_enable_gc(gc_enabled, sst_layer.object_store().info().scheme())
        && matches!(path_type, PathType::Data | PathType::Bare)
    {
        Arc::new(ObjectStoreFilePurger { file_ref_manager })
    } else {
        Arc::new(LocalFilePurger::new(scheduler, sst_layer, cache_manager))
    }
}

/// Creates a local file purger that deletes files from both the storage and the cache.
pub fn create_local_file_purger(
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    _file_ref_manager: FileReferenceManagerRef,
) -> FilePurgerRef {
    Arc::new(LocalFilePurger::new(scheduler, sst_layer, cache_manager))
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
        }
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

    fn delete_index(&self, file_meta: FileMeta) {
        let sst_layer = self.sst_layer.clone();
        let cache_manager = self.cache_manager.clone();
        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            let index_id = file_meta.index_id();
            if let Err(e) = delete_index(index_id, &sst_layer, &cache_manager).await {
                error!(e; "Failed to delete index for file {:?} from storage", file_meta);
            }
        })) {
            error!(e; "Failed to schedule the index purge request");
        }
    }
}

impl FilePurger for LocalFilePurger {
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool, index_outdated: bool) {
        if is_delete {
            self.delete_file(file_meta);
        } else if index_outdated {
            self.delete_index(file_meta);
        }
    }
}

#[derive(Debug)]
pub struct ObjectStoreFilePurger {
    file_ref_manager: FileReferenceManagerRef,
}

impl FilePurger for ObjectStoreFilePurger {
    fn remove_file(&self, file_meta: FileMeta, _is_delete: bool, _index_outdated: bool) {
        // if not on local file system, instead inform the global file purger to remove the file reference.
        // notice that no matter whether the file is deleted or not, we need to remove the reference
        // because the file is no longer in use nonetheless.
        // for same reason, we don't care about index_outdated here.
        self.file_ref_manager.remove_file(&file_meta);
    }

    fn new_file(&self, file_meta: &FileMeta) {
        self.file_ref_manager.add_file(file_meta);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use common_test_util::temp_dir::create_temp_dir;
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

    #[tokio::test]
    async fn test_file_purge() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file-purge");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let sst_dir = "table1";

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let object_store = ObjectStore::new(builder).unwrap().finish();

        let layer = Arc::new(AccessLayer::new(
            sst_dir,
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));
        let path = location::sst_file_path(sst_dir, sst_file_id, layer.path_type());
        object_store.write(&path, vec![0; 4096]).await.unwrap();

        let scheduler = Arc::new(LocalScheduler::new(3));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer, None));

        {
            let handle = FileHandle::new(
                FileMeta {
                    region_id: sst_file_id.region_id(),
                    file_id: sst_file_id.file_id(),
                    time_range: FileTimeRange::default(),
                    level: 0,
                    file_size: 4096,
                    max_row_group_uncompressed_size: 4096,
                    available_indexes: Default::default(),
                    indexes: Default::default(),
                    index_file_size: 0,
                    index_version: 0,
                    num_rows: 0,
                    num_row_groups: 0,
                    sequence: None,
                    partition_expr: None,
                    num_series: 0,
                },
                file_purger,
            );
            // mark file as deleted and drop the handle, we expect the file is deleted.
            handle.mark_deleted();
        }

        scheduler.stop(true).await.unwrap();

        assert!(!object_store.exists(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_purge_with_index() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file-purge");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_file_id = RegionIndexId::new(sst_file_id, 0);
        let sst_dir = "table1";

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let object_store = ObjectStore::new(builder).unwrap().finish();

        let layer = Arc::new(AccessLayer::new(
            sst_dir,
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));
        let path = location::sst_file_path(sst_dir, sst_file_id, layer.path_type());
        object_store.write(&path, vec![0; 4096]).await.unwrap();

        let index_path = location::index_file_path(sst_dir, index_file_id, layer.path_type());
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let scheduler = Arc::new(LocalScheduler::new(3));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer, None));

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
                    }],
                    index_file_size: 4096,
                    index_version: 0,
                    num_rows: 1024,
                    num_row_groups: 1,
                    sequence: NonZeroU64::new(4096),
                    partition_expr: None,
                    num_series: 0,
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
