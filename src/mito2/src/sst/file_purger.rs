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

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::{Arc, Mutex};

use common_telemetry::{error, info};
use serde::{Deserialize, Serialize};
use store_api::storage::{RegionId, TableId};

use crate::access_layer::AccessLayerRef;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::CacheManagerRef;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::{FileId, FileMeta};

/// Request to remove a file.
#[derive(Debug)]
pub struct PurgeRequest {
    /// File meta.
    pub file_meta: FileMeta,
}

/// A worker to delete files in background.
pub trait FilePurger: Send + Sync + fmt::Debug {
    /// Send a purge request to the background worker.
    fn send_request(&self, request: PurgeRequest);

    fn add_new_file(&self, _: &FileMeta) {
        // noop
    }
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

/// A no-op file purger can be used in combination with reading SST files outside of this region.
#[derive(Debug)]
pub struct NoopFilePurger;

impl FilePurger for NoopFilePurger {
    fn send_request(&self, _: PurgeRequest) {
        // noop
    }
}

/// Purger that purges file for current region.
pub struct LocalFilePurger {
    scheduler: SchedulerRef,
    sst_layer: AccessLayerRef,
    cache_manager: Option<CacheManagerRef>,
    file_ref_manager: FileReferenceManagerRef,
    /// Whether the underlying object store is local filesystem.
    /// if it is, we can delete the file directly.
    /// Otherwise, we should inform the global file ref manager to delete the file.
    is_local_fs: bool,
}

impl fmt::Debug for LocalFilePurger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalFilePurger")
            .field("sst_layer", &self.sst_layer)
            .finish()
    }
}

impl LocalFilePurger {
    /// Creates a new purger.
    pub fn new(
        scheduler: SchedulerRef,
        sst_layer: AccessLayerRef,
        cache_manager: Option<CacheManagerRef>,
        file_ref_manager: FileReferenceManagerRef,
    ) -> Self {
        let is_local_fs = sst_layer.object_store().info().scheme() == object_store::Scheme::Fs;
        Self {
            scheduler,
            sst_layer,
            cache_manager,
            file_ref_manager,
            is_local_fs,
        }
    }

    /// Deletes the file(and it's index, if any) from cache and storage.
    fn delete_file(&self, file_meta: FileMeta) {
        let sst_layer = self.sst_layer.clone();

        // Remove meta of the file from cache.
        if let Some(cache) = &self.cache_manager {
            cache.remove_parquet_meta_data(file_meta.file_id());
        }

        let cache_manager = self.cache_manager.clone();
        if let Err(e) = self.scheduler.schedule(Box::pin(async move {
            if let Err(e) = sst_layer.delete_sst(&file_meta).await {
                error!(e; "Failed to delete SST file, file_id: {}, region: {}",
                    file_meta.file_id, file_meta.region_id);
            } else {
                info!(
                    "Successfully deleted SST file, file_id: {}, region: {}",
                    file_meta.file_id, file_meta.region_id
                );
            }

            if let Some(write_cache) = cache_manager.as_ref().and_then(|cache| cache.write_cache())
            {
                // Removes index file from the cache.
                if file_meta.exists_index() {
                    write_cache
                        .remove(IndexKey::new(
                            file_meta.region_id,
                            file_meta.file_id,
                            FileType::Puffin,
                        ))
                        .await;
                }
                // Remove the SST file from the cache.
                write_cache
                    .remove(IndexKey::new(
                        file_meta.region_id,
                        file_meta.file_id,
                        FileType::Parquet,
                    ))
                    .await;
            }

            // Purges index content in the stager.
            if let Err(e) = sst_layer
                .puffin_manager_factory()
                .purge_stager(file_meta.file_id())
                .await
            {
                error!(e; "Failed to purge stager with index file, file_id: {}, region: {}",
                    file_meta.file_id(), file_meta.region_id);
            }
        })) {
            error!(e; "Failed to schedule the file purge request");
        }
    }
}

impl FilePurger for LocalFilePurger {
    fn send_request(&self, request: PurgeRequest) {
        if self.is_local_fs {
            self.delete_file(request.file_meta);
        } else {
            // TODO(discord9): instead inform the global file purger to delete the file.
        }
    }

    fn add_new_file(&self, file_meta: &FileMeta) {
        self.file_ref_manager
            .add_file(file_meta, self.sst_layer.clone());
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileRef {
    pub region_id: RegionId,
    pub file_id: FileId,
}

impl FileRef {
    pub fn new(region_id: RegionId, file_id: FileId) -> Self {
        Self { region_id, file_id }
    }
}

/// Returns the path of the tmp ref file for given table id and datanode id.
pub fn ref_file_path(table_id: TableId, node_id: u64) -> String {
    format!("{}/{}/{:020}.refs", table_id, PURGER_REFS_PATH, node_id)
}

/// Manages all file references in one datanode.
/// It keeps track of which files are referenced and group by table ids.
/// And periodically update the references to tmp file in object storage.
/// This is useful for ensuring that files are not deleted while they are still in use by any
/// query.
#[derive(Debug, Default)]
pub struct FileReferenceManager {
    /// Datanode id. used to determine tmp ref file name.
    node_id: u64,
    files_per_table: Mutex<HashMap<TableId, HashSet<FileRef>>>,
    /// Access layer per table. Will be used to upload tmp file for recording references
    /// to the object storage.
    /// Also will be cleaned up if corresponding table is dropped and manifest become empty.
    table_access_layers: Mutex<HashMap<TableId, AccessLayerRef>>,
}

const PURGER_REFS_PATH: &str = ".purger_refs";

pub type FileReferenceManagerRef = Arc<FileReferenceManager>;

impl FileReferenceManager {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn clean_dropped_tables(&self) {
        todo!("Remove tables's access layer which have empty manifest meaning they are dropped.");
    }

    pub async fn upload_ref_file(&self) {
        let files_per_table = {
            let guard = self.files_per_table.lock().unwrap();
            guard.clone()
        };
        for (table_id, file_refs) in files_per_table {
            if file_refs.is_empty() {
                continue;
            }
            let access_layer = {
                let guard = self.table_access_layers.lock().unwrap();
                guard.get(&table_id).cloned()
            };
            if access_layer.is_none() {
                error!(
                    "No access layer found for table {}, skip uploading ref file",
                    table_id
                );
                continue;
            }
            let access_layer = access_layer.unwrap();
            let path = ref_file_path(table_id, self.node_id);
            let content = serde_json::to_vec(&file_refs).unwrap_or_default();
            if let Err(e) = access_layer.object_store().write(&path, content).await {
                error!(e; "Failed to upload ref file to {}, table {}", path, table_id);
            } else {
                info!(
                    "Successfully uploaded ref file to {}, table {}",
                    path, table_id
                );
            }
        }
    }

    pub fn add_file(&self, file_meta: &FileMeta, table_access_layer: AccessLayerRef) {
        let table_id = file_meta.region_id.table_id();
        {
            let mut guard = self.files_per_table.lock().unwrap();
            let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
            guard.entry(table_id).or_default().insert(file_ref);
        }
        {
            let mut guard = self.table_access_layers.lock().unwrap();
            guard
                .entry(table_id)
                .or_insert_with(|| table_access_layer.clone());
        }
    }

    pub fn remove_file(&self, file_meta: &FileMeta) {
        let table_id = file_meta.region_id.table_id();
        let file_ref = FileRef::new(file_meta.region_id, file_meta.file_id);
        let mut guard = self.files_per_table.lock().unwrap();
        guard.entry(table_id).or_default().remove(&file_ref);
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU64;

    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;
    use smallvec::SmallVec;
    use store_api::region_request::PathType;
    use store_api::storage::RegionId;

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::schedule::scheduler::{LocalScheduler, Scheduler};
    use crate::sst::file::{FileHandle, FileId, FileMeta, FileTimeRange, IndexType, RegionFileId};
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

        let file_purger = Arc::new(LocalFilePurger::new(
            scheduler.clone(),
            layer,
            None,
            Arc::new(Default::default()),
        ));

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
                    num_rows: 0,
                    num_row_groups: 0,
                    sequence: None,
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

        let file_purger = Arc::new(LocalFilePurger::new(
            scheduler.clone(),
            layer,
            None,
            Arc::new(Default::default()),
        ));

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
                    num_rows: 1024,
                    num_row_groups: 1,
                    sequence: NonZeroU64::new(4096),
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
