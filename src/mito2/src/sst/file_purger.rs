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

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};

use common_telemetry::error;
use store_api::storage::{FileId, IndexVersion};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::error::Result;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileMeta, RegionIndexId, delete_files};
use crate::sst::file_ref::FileReferenceManagerRef;

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
    index_version_rc: Mutex<HashMap<(FileId, IndexVersion), usize>>,
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
            index_version_rc: Mutex::new(HashMap::new()),
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

    fn delete_old_index(&self, file_meta: FileMeta, old_version: IndexVersion) {
        if file_meta.index_version == 0 {
            // no index to delete
            return;
        }
        let sst_layer = self.sst_layer.clone();
        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            let index_id = RegionIndexId::new(file_meta.file_id(), old_version);
            if let Err(e) = sst_layer.delete_index(&index_id).await {
                error!(e; "Failed to delete index {:?} from storage", index_id);
            }
        })) {
            error!(e; "Failed to schedule the index purge request");
        }
    }

    /// Remove reference count for old index version.
    /// Returns true if the old index can be deleted.
    fn rm_old_index_rc(&self, file_id: FileId, old_version: IndexVersion) -> bool {
        let mut index_version = self.index_version_rc.lock().unwrap();
        let mut should_rm = false;
        index_version
            .entry((file_id, old_version))
            .and_modify(|count| {
                if *count > 0 {
                    *count -= 1;
                }
                if *count == 0 {
                    should_rm = true;
                }
            });
        should_rm
    }

    fn new_index_rc(&self, file_id: FileId, new_version: IndexVersion) {
        let mut index_version = self.index_version_rc.lock().unwrap();
        index_version
            .entry((file_id, new_version))
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }
}

impl FilePurger for LocalFilePurger {
    fn remove_file(&self, file_meta: FileMeta, is_delete: bool) {
        if is_delete {
            self.rm_old_index_rc(file_meta.file_id, file_meta.index_version);
            self.delete_file(file_meta);
        } else {
            let version = file_meta.index_version;
            self.update_index(file_meta, version);
        }
    }

    fn update_index(&self, file_meta: FileMeta, old_version: IndexVersion) {
        let should_delete_old_index = self.rm_old_index_rc(file_meta.file_id, old_version);
        if should_delete_old_index {
            common_telemetry::debug!(
                "Deleting old index version {} for file {:?}",
                old_version,
                file_meta.file_id
            );
            self.delete_old_index(file_meta, old_version);
        }
    }

    fn new_file(&self, file_meta: &FileMeta) {
        self.new_index_rc(file_meta.file_id, file_meta.index_version);
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

    #[tokio::test]
    async fn test_index_reference_counting() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("index-rc-test");
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        let file_id = sst_file_id.file_id();
        let index_version = 1;

        // Test new_index_rc increments count
        file_purger.new_index_rc(file_id, index_version);
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(file_id, index_version)), Some(&1));
        }

        // Test new_index_rc increments count for existing entry
        file_purger.new_index_rc(file_id, index_version);
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(file_id, index_version)), Some(&2));
        }

        // Test rm_old_index_rc decrements count
        let should_delete = file_purger.rm_old_index_rc(file_id, index_version);
        assert!(!should_delete); // Should not delete when count > 0
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(file_id, index_version)), Some(&1));
        }

        // Test rm_old_index_rc returns true when count reaches 0
        let should_delete = file_purger.rm_old_index_rc(file_id, index_version);
        assert!(should_delete); // Should delete when count reaches 0
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(file_id, index_version)), Some(&0));
        }

        // Test rm_old_index_rc for non-existent entry
        let non_existent_file_id = FileId::random();
        let should_delete = file_purger.rm_old_index_rc(non_existent_file_id, 999);
        assert!(!should_delete); // Should return false for non-existent entry
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            // Should not insert a new entry for non-existent file
            assert!(!rc_map.contains_key(&(non_existent_file_id, 999)));
        }
    }

    #[tokio::test]
    async fn test_update_index_behavior() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("update-index-test");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let old_index_id = RegionIndexId::new(sst_file_id, 1);
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer.clone(), None);

        // Create an old index file
        let old_index_path = location::index_file_path(sst_dir, old_index_id, layer.path_type());
        object_store
            .write(&old_index_path, vec![0; 4096])
            .await
            .unwrap();

        // Create file meta with updated index version
        let file_meta = FileMeta {
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
            index_version: 2, // Updated version
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Set up reference count for old index
        file_purger.new_index_rc(sst_file_id.file_id(), 1);

        // Call update_index which should delete the old index
        file_purger.update_index(file_meta.clone(), 1);

        // Wait for the scheduled task to complete
        scheduler.stop(true).await.unwrap();

        // Verify old index is deleted
        assert!(!object_store.exists(&old_index_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_new_file_behavior() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("new-file-test");
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Create file meta with index
        let file_meta = FileMeta {
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
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Call new_file which should increment reference count
        file_purger.new_file(&file_meta);

        // Verify reference count is incremented
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(sst_file_id.file_id(), 1)), Some(&1));
        }

        // Call new_file again with same file
        file_purger.new_file(&file_meta);

        // Verify reference count is incremented again
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(sst_file_id.file_id(), 1)), Some(&2));
        }
    }

    #[tokio::test]
    async fn test_remove_file_with_delete() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("remove-file-delete-test");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let index_file_id = RegionIndexId::new(sst_file_id, 1);
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer.clone(), None);

        // Create SST file and index file
        let sst_path = location::sst_file_path(sst_dir, sst_file_id, layer.path_type());
        let index_path = location::index_file_path(sst_dir, index_file_id, layer.path_type());
        object_store.write(&sst_path, vec![0; 4096]).await.unwrap();
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        // Create file meta
        let file_meta = FileMeta {
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
            index_version: 1,
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Set up reference count for index
        file_purger.new_index_rc(sst_file_id.file_id(), 1);

        // Call remove_file with is_delete=true
        file_purger.remove_file(file_meta, true);

        // Wait for the scheduled task to complete
        scheduler.stop(true).await.unwrap();

        // Verify both files are deleted
        assert!(!object_store.exists(&sst_path).await.unwrap());
        assert!(!object_store.exists(&index_path).await.unwrap());
    }

    #[tokio::test]
    async fn test_remove_file_without_delete() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("remove-file-no-delete-test");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = RegionFileId::new(RegionId::new(0, 0), FileId::random());
        let old_index_id = RegionIndexId::new(sst_file_id, 1);
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer.clone(), None);

        // Create old index file
        let old_index_path = location::index_file_path(sst_dir, old_index_id, layer.path_type());
        object_store
            .write(&old_index_path, vec![0; 4096])
            .await
            .unwrap();

        // Create file meta with updated index version
        let file_meta = FileMeta {
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
            index_version: 2, // Updated version
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Set up reference count for old index
        file_purger.new_index_rc(sst_file_id.file_id(), 1);

        // Call remove_file with is_delete=false
        file_purger.remove_file(file_meta, false);

        // Wait for the scheduled task to complete
        scheduler.stop(true).await.unwrap();

        // Verify old index exist and reference count is also there
        assert!(object_store.exists(&old_index_path).await.unwrap());
        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(sst_file_id.file_id(), 1)), Some(&1));
            assert!(rc_map.get(&(sst_file_id.file_id(), 2)).is_none());
        }
    }

    #[tokio::test]
    async fn test_edge_cases() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("edge-cases-test");
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

        let scheduler = Arc::new(LocalScheduler::new(3));
        let file_purger = LocalFilePurger::new(scheduler.clone(), layer, None);

        // Test with index_version 0 (no index)
        let file_meta_no_index = FileMeta {
            region_id: sst_file_id.region_id(),
            file_id: sst_file_id.file_id(),
            time_range: FileTimeRange::default(),
            level: 0,
            file_size: 4096,
            available_indexes: Default::default(),
            indexes: Default::default(),
            index_file_size: 0,
            index_version: 0, // No index
            num_rows: 1024,
            num_row_groups: 1,
            sequence: NonZeroU64::new(4096),
            partition_expr: None,
            num_series: 0,
        };

        // Call delete_old_index with index_version 0
        file_purger.delete_old_index(file_meta_no_index.clone(), 0);

        // Test update_index with index_version 0
        file_purger.update_index(file_meta_no_index.clone(), 0);

        // Test multiple reference counts and multiple decrements
        let file_id = sst_file_id.file_id();
        let index_version = 5;

        // Add multiple references
        for _ in 0..5 {
            file_purger.new_index_rc(file_id, index_version);
        }

        // Remove all references
        for i in 0..5 {
            let should_delete = file_purger.rm_old_index_rc(file_id, index_version);
            // Should only delete on the last removal (when count reaches 0)
            assert_eq!(should_delete, i == 4);
        }

        // Test with different file IDs and versions
        let file_id2 = FileId::random();
        file_purger.new_index_rc(file_id2, 1);
        file_purger.new_index_rc(file_id2, 2);
        file_purger.new_index_rc(file_id, 3);

        {
            let rc_map = file_purger.index_version_rc.lock().unwrap();
            assert_eq!(rc_map.get(&(file_id2, 1)), Some(&1));
            assert_eq!(rc_map.get(&(file_id2, 2)), Some(&1));
            assert_eq!(rc_map.get(&(file_id, 3)), Some(&1));
        }

        scheduler.stop(true).await.unwrap();
    }
}
