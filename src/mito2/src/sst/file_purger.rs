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

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileMeta, delete_files};
use crate::sst::file_ref::FileReferenceManagerRef;

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync + fmt::Debug {
    /// Send a request to remove the file.
    /// If `is_delete` is true, the file will be deleted from the storage.
    /// Otherwise, only the reference will be removed.
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool);

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
    fn remove_file(&self, _file_meta: FileMeta, _is_delete: bool) {
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

pub fn is_local_fs(sst_layer: &AccessLayerRef) -> bool {
    sst_layer.object_store().info().scheme() == object_store::Scheme::Fs
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
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
) -> FilePurgerRef {
    if is_local_fs(&sst_layer) {
        Arc::new(LocalFilePurger::new(scheduler, sst_layer, cache_manager))
    } else {
        Arc::new(ObjectStoreFilePurger { file_ref_manager })
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
                &[file_meta.file_id],
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
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool) {
        if is_delete {
            self.delete_file(file_meta);
        }
    }
}

#[derive(Debug)]
pub struct ObjectStoreFilePurger {
    file_ref_manager: FileReferenceManagerRef,
}

impl FilePurger for ObjectStoreFilePurger {
    fn remove_file(&self, file_meta: FileMeta, _is_delete: bool) {
        // if not on local file system, instead inform the global file purger to remove the file reference.
        // notice that no matter whether the file is deleted or not, we need to remove the reference
        // because the file is no longer in use nonetheless.
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
    use crate::sst::file::{FileHandle, FileMeta, FileTimeRange, IndexType, RegionFileId};
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
                    available_indexes: Default::default(),
                    index_file_size: 0,
                    index_file_id: None,
                    num_rows: 0,
                    num_row_groups: 0,
                    sequence: None,
                    partition_expr: None,
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

        let index_path = location::index_file_path(sst_dir, sst_file_id, layer.path_type());
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
                    available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
                    index_file_size: 4096,
                    index_file_id: None,
                    num_rows: 1024,
                    num_row_groups: 1,
                    sequence: NonZeroU64::new(4096),
                    partition_expr: None,
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
