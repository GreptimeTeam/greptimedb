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
use dashmap::DashMap;
use store_api::storage::IndexVersion;

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileMeta, RegionIndexId, delete_files, delete_index};
use crate::sst::file_ref::FileReferenceManagerRef;

/// Index file reference tracking information
#[derive(Debug, Default)]
struct IndexRefInfo {
    ref_count: usize,
    is_deleted: bool,
}

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync + fmt::Debug {
    /// Send a request to remove the file.
    /// If `is_delete` is true, the file will be deleted from the storage.
    /// Otherwise, only the reference will be removed.
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool);

    /// Update index version of the file. The new `FileMeta` contains the updated index version.
    /// `old_version` is the previous index version before update.
    fn update_index(&self, file_meta: FileMeta, old_version: IndexVersion);

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

    fn update_index(&self, _file_meta: FileMeta, _old_version: IndexVersion) {
        // noop
    }
}

/// Purger that purges file for current region.
pub struct LocalFilePurger {
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    /// Maps RegionIndexId to (ref_count, is_deleted) for index files
    index_refs: Arc<DashMap<RegionIndexId, IndexRefInfo>>,
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
    gc_enabled: bool,
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
) -> FilePurgerRef {
    if gc_enabled && !is_local_fs(&sst_layer) {
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
            index_refs: Arc::new(DashMap::new()),
        }
    }

    /// Stop the scheduler of the file purger.
    pub async fn stop_scheduler(&self) -> Result<()> {
        self.scheduler.stop(true).await
    }

    /// Add a reference to an index file
    pub fn add_index_reference(&self, index_id: RegionIndexId) {
        self.index_refs
            .entry(index_id)
            .and_modify(|info| {
                info.ref_count += 1;
            })
            .or_insert(IndexRefInfo {
                ref_count: 1,
                is_deleted: false,
            });
    }

    /// Remove a reference to an index file, returns should_delete
    pub fn remove_index_reference(&self, index_id: &RegionIndexId) -> bool {
        let mut should_delete = false;

        if let Some(mut info) = self.index_refs.get_mut(index_id) {
            info.ref_count -= 1;
            should_delete = info.is_deleted && info.ref_count == 0;

            if info.ref_count == 0 {
                // Remove from map if no more references
                drop(info); // Release the lock before removing
                self.index_refs.remove(index_id);
            }
        }

        should_delete
    }

    /// Mark an index file as deleted (but don't actually delete yet)
    pub fn mark_index_deleted(&self, index_id: &RegionIndexId) -> bool {
        let should_delete;

        if let Some(mut info) = self.index_refs.get_mut(index_id) {
            info.is_deleted = true;
            should_delete = info.ref_count == 0;

            if should_delete {
                // Remove from map if no references
                drop(info); // Release the lock before removing
                self.index_refs.remove(index_id);
            }
        } else {
            // No references exist, can delete immediately
            should_delete = true;
        }

        should_delete
    }

    /// Check and delete index if it's marked deleted and has no references
    fn check_and_delete_index(&self, file_meta: FileMeta, index_version: IndexVersion) {
        let index_id = RegionIndexId::new(file_meta.file_id(), index_version);
        if self.mark_index_deleted(&index_id) {
            self.delete_index(file_meta, index_version);
        }
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

    fn delete_index(&self, file_meta: FileMeta, old_version: IndexVersion) {
        if file_meta.index_version == 0 {
            // no index to delete
            return;
        }
        let sst_layer = self.sst_layer.clone();
        let cache_manager = self.cache_manager.clone();
        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            let index_id = RegionIndexId::new(file_meta.file_id(), old_version);
            if let Err(e) = delete_index(
                file_meta.region_id,
                file_meta.file_id,
                old_version,
                &sst_layer,
                &cache_manager,
            )
            .await
            {
                error!(e; "Failed to delete index {:?} from storage", index_id);
            }
        })) {
            error!(e; "Failed to schedule the index purge request");
        }
    }
}

impl FilePurger for LocalFilePurger {
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool) {
        // Release reference to current index version if it exists
        if file_meta.exists_index() {
            let index_id = RegionIndexId::new(file_meta.file_id(), file_meta.index_version);
            let should_delete = self.remove_index_reference(&index_id);

            // If this index was marked for deletion and ref count reached 0, delete it
            if should_delete {
                self.delete_index(file_meta.clone(), file_meta.index_version);
            }
        }

        if is_delete {
            self.delete_file(file_meta);
        }
    }

    fn update_index(&self, file_meta: FileMeta, old_version: IndexVersion) {
        // Mark old index as deleted, but only actually delete if no references
        self.check_and_delete_index(file_meta, old_version);
    }

    fn new_file(&self, file_meta: &FileMeta) {
        if file_meta.exists_index() {
            self.add_index_reference(RegionIndexId::new(
                file_meta.file_id(),
                file_meta.index_version,
            ));
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
        // TODO(discord9): consider impl a .tombstone file to reduce files needed to list
    }

    fn update_index(&self, _file_meta: FileMeta, _old_version: IndexVersion) {
        // nothing need to do for object store
        // as new file reference with new index version will be added when new `FileHandle` is created
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

    /// Tests for Index Reference Counting Logic
    #[tokio::test]
    async fn test_add_index_reference() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-add-index-ref");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Test adding a new index reference
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);

        file_purger.add_index_reference(index_id);

        {
            // Check that the reference was added
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 1);
            assert!(!info.is_deleted);
        }

        // Test adding another reference to the same index
        file_purger.add_index_reference(index_id);

        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 2);
            assert!(!info.is_deleted);
        }

        scheduler.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_remove_index_reference() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-remove-index-ref");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Add an index reference first
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        file_purger.add_index_reference(index_id);
        file_purger.add_index_reference(index_id); // Add twice for ref_count = 2

        // Remove one reference
        let should_delete = file_purger.remove_index_reference(&index_id);
        assert!(!should_delete); // Should not delete as ref_count is still 1

        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 1);
        } // info is dropped here

        // Remove the second reference
        let should_delete = file_purger.remove_index_reference(&index_id);
        assert!(!should_delete); // Should not delete as is_deleted is false

        // Index should be removed from map as ref_count is 0
        assert!(file_purger.index_refs.get(&index_id).is_none());

        scheduler.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_remove_index_reference_with_deleted_flag() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-remove-index-ref-deleted");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Add an index reference
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        file_purger.add_index_reference(index_id);

        // Mark as deleted
        let should_delete = file_purger.mark_index_deleted(&index_id);
        assert!(!should_delete); // Should not delete as ref_count is 1

        // Remove the reference
        let should_delete = file_purger.remove_index_reference(&index_id);
        assert!(should_delete); // Should delete as is_deleted is true and ref_count is 0

        // Index should be removed from map
        assert!(file_purger.index_refs.get(&index_id).is_none());

        scheduler.stop(true).await.unwrap();
    }

    // Tests for Deletion Logic

    #[tokio::test]
    async fn test_mark_index_deleted() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-mark-index-deleted");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Test marking a non-existent index as deleted
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        let should_delete = file_purger.mark_index_deleted(&index_id);
        assert!(should_delete); // Should delete immediately as no references exist

        // Test marking an existing index as deleted
        file_purger.add_index_reference(index_id);
        let should_delete = file_purger.mark_index_deleted(&index_id);
        assert!(!should_delete); // Should not delete as ref_count is 1

        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert!(info.is_deleted);
            assert_eq!(info.ref_count, 1);
        } // info is dropped here

        scheduler.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_check_and_delete_index() {
        common_telemetry::init_default_ut_logging();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-check-delete-index");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Create an index file
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        let index_path = location::index_file_path("table1", index_id, PathType::Bare);
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        // Create a file meta with the index
        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Add reference and check that index is not deleted
        file_purger.add_index_reference(index_id);
        file_purger.check_and_delete_index(file_meta.clone(), 1);

        // Index should still exist
        assert!(object_store.exists(&index_path).await.unwrap());

        // Remove reference and mark as deleted
        file_purger.remove_index_reference(&index_id);
        file_purger.check_and_delete_index(file_meta, 1);

        // Wait for async deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Index should be deleted
        assert!(!object_store.exists(&index_path).await.unwrap());

        scheduler.stop(true).await.unwrap();
    }

    // Tests for FileHandle Lifecycle Integration

    #[tokio::test]
    async fn test_file_handle_lifecycle_with_index() {
        common_telemetry::init_default_ut_logging();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-file-handle-lifecycle");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer.clone(), None));

        // Create a file with index
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        let file_path = location::sst_file_path("table1", file_id, PathType::Bare);
        let index_path = location::index_file_path("table1", index_id, PathType::Bare);

        object_store.write(&file_path, vec![0; 4096]).await.unwrap();
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Create FileHandle - this should add a reference
        let handle = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Check that reference was added
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 1);
            assert!(!info.is_deleted);
        } // info is dropped here

        // Clone the handle to increase reference count
        let handle2 = handle.clone();

        // Check that reference count is still 1 (only one FileHandleInner)
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 1);
        } // info is dropped here

        // Mark file as deleted
        handle.mark_deleted();

        // Drop one handle - should not delete yet
        drop(handle);

        // File and index should still exist
        assert!(object_store.exists(&file_path).await.unwrap());
        assert!(object_store.exists(&index_path).await.unwrap());

        // Drop the second handle - should delete now
        drop(handle2);

        // Wait for async deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // File and index should be deleted
        assert!(!object_store.exists(&file_path).await.unwrap());
        assert!(!object_store.exists(&index_path).await.unwrap());

        scheduler.stop(true).await.unwrap();
    }

    #[tokio::test]
    async fn test_update_index_version() {
        common_telemetry::init_default_ut_logging();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-update-index-version");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer.clone(), None));

        // Create a file with index version 1
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let old_index_id = RegionIndexId::new(file_id, 1);
        let new_index_id = RegionIndexId::new(file_id, 2);
        let old_index_path = location::index_file_path("table1", old_index_id, PathType::Bare);
        let new_index_path = location::index_file_path("table1", new_index_id, PathType::Bare);

        object_store
            .write(&old_index_path, vec![0; 4096])
            .await
            .unwrap();
        object_store
            .write(&new_index_path, vec![0; 4096])
            .await
            .unwrap();

        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 2, // New version
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Create FileHandle with new index version - this should add reference to new index
        let handle = FileHandle::new(file_meta.clone(), file_purger.clone());

        // Check that reference was added to new index
        {
            let info = file_purger.index_refs.get(&new_index_id).unwrap();
            assert_eq!(info.ref_count, 1);
            assert!(!info.is_deleted);
        } // info is dropped here

        // Update index - this should mark old index as deleted
        file_purger.update_index(file_meta.clone(), 1);

        // Check that old index is marked as deleted
        {
            let info = file_purger.index_refs.get(&old_index_id);
            // Old index should not exist as it had no references
            assert!(info.is_none());
        } // info is dropped here

        // New index should still have its reference
        {
            let info = file_purger.index_refs.get(&new_index_id).unwrap();
            assert_eq!(info.ref_count, 1);
            assert!(!info.is_deleted);
        } // info is dropped here

        // Drop the handle
        drop(handle);

        // Wait for async operations
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // New index should still exist as it was the current one
        assert!(object_store.exists(&new_index_path).await.unwrap());

        scheduler.stop(true).await.unwrap();
    }

    // Tests for Edge Cases and Error Conditions

    #[tokio::test]
    async fn test_concurrent_index_operations() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-concurrent-index-ops");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler, layer, None));

        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);

        let mut handles = vec![];

        // Spawn multiple threads to add references concurrently
        for _ in 0..10 {
            let purger = file_purger.clone();
            let idx = index_id;
            let handle = tokio::spawn(async move {
                purger.add_index_reference(idx);
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Check that all 10 references were added
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 10);
            assert!(!info.is_deleted);
        } // info is dropped here

        // Spawn multiple threads to remove references concurrently
        let mut handles = vec![];
        for _ in 0..10 {
            let purger = file_purger.clone();
            let idx = index_id;
            let handle = tokio::spawn(async move {
                purger.remove_index_reference(&idx);
            });
            handles.push(handle);
        }

        // Wait for all operations to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Index should be removed as ref_count is 0
        assert!(file_purger.index_refs.get(&index_id).is_none());
    }

    #[tokio::test]
    async fn test_multiple_file_handles_same_index() {
        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-multiple-handles-same-index");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store,
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler, layer, None));

        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);

        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Create multiple FileHandles for the same file
        let handle1 = FileHandle::new(file_meta.clone(), file_purger.clone());
        let handle2 = FileHandle::new(file_meta.clone(), file_purger.clone());
        let handle3 = FileHandle::new(file_meta, file_purger.clone());

        // Each FileHandle should add a reference
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 3);
        } // info is dropped here

        // Mark all handles as deleted
        handle1.mark_deleted();
        handle2.mark_deleted();
        handle3.mark_deleted();

        // Drop one handle - should not delete yet
        drop(handle1);
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 2);
        } // info is dropped here

        // Drop another handle - should not delete yet
        drop(handle2);
        {
            let info = file_purger.index_refs.get(&index_id).unwrap();
            assert_eq!(info.ref_count, 1);
        } // info is dropped here

        // Drop the last handle - should delete now
        drop(handle3);

        // Index should be removed from map
        assert!(file_purger.index_refs.get(&index_id).is_none());
    }

    #[tokio::test]
    async fn test_error_handling_during_deletion() {
        common_telemetry::init_default_ut_logging();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-error-handling-deletion");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer.clone(), None));

        // Create a file with index
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        let file_path = location::sst_file_path("table1", file_id, PathType::Bare);
        let index_path = location::index_file_path("table1", index_id, PathType::Bare);

        object_store.write(&file_path, vec![0; 4096]).await.unwrap();
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Create FileHandle
        let handle = FileHandle::new(file_meta, file_purger.clone());

        // Manually delete the index file to simulate an error during deletion
        object_store.delete(&index_path).await.unwrap();

        // Mark file as deleted and drop handle
        handle.mark_deleted();
        drop(handle);

        // Wait for async deletion - should handle error gracefully
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // File should still be deleted despite index deletion error
        assert!(!object_store.exists(&file_path).await.unwrap());

        scheduler.stop(true).await.unwrap();
    }

    // Tests for Integration with Storage Layer

    #[tokio::test]
    async fn test_local_file_system_integration() {
        common_telemetry::init_default_ut_logging();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let dir = create_temp_dir("test-local-fs-integration");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let object_store = ObjectStore::new(builder).unwrap().finish();

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let layer = Arc::new(AccessLayer::new(
            "table1",
            PathType::Bare,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer.clone(), None));

        // Create a file with index
        let file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_id = RegionIndexId::new(file_id, 1);
        let file_path = location::sst_file_path("table1", file_id, PathType::Bare);
        let index_path = location::index_file_path("table1", index_id, PathType::Bare);

        object_store.write(&file_path, vec![0; 4096]).await.unwrap();
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let file_meta = FileMeta {
            region_id: file_id.region_id(),
            file_id: file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            indexes: vec![ColumnIndexMetadata {
                column_id: 0,
                created_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
            }],
            index_file_size: 4096,
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Create FileHandle
        let handle = FileHandle::new(file_meta, file_purger.clone());

        // Verify files exist
        assert!(object_store.exists(&file_path).await.unwrap());
        assert!(object_store.exists(&index_path).await.unwrap());

        // Mark file as deleted and drop handle
        handle.mark_deleted();
        drop(handle);

        // Wait for async deletion
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify files are deleted
        assert!(!object_store.exists(&file_path).await.unwrap());
        assert!(!object_store.exists(&index_path).await.unwrap());

        scheduler.stop(true).await.unwrap();
    }
}
