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

use common_telemetry::{error, info};

use crate::access_layer::AccessLayerRef;
use crate::cache::CacheManagerRef;
use crate::schedule::scheduler::SchedulerRef;
use crate::sst::file::FileMeta;

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
}

pub type FilePurgerRef = Arc<dyn FilePurger>;

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
}

impl FilePurger for LocalFilePurger {
    fn send_request(&self, request: PurgeRequest) {
        let file_meta = request.file_meta;
        let sst_layer = self.sst_layer.clone();

        // Remove meta of the file from cache.
        if let Some(cache) = &self.cache_manager {
            cache.remove_parquet_meta_data(file_meta.region_id, file_meta.file_id);
        }

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
        })) {
            error!(e; "Failed to schedule the file purge request");
        }
    }
}

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::create_temp_dir;
    use object_store::services::Fs;
    use object_store::ObjectStore;
    use smallvec::SmallVec;

    use super::*;
    use crate::access_layer::AccessLayer;
    use crate::schedule::scheduler::{LocalScheduler, Scheduler};
    use crate::sst::file::{FileHandle, FileId, FileMeta, FileTimeRange, IndexType};
    use crate::sst::index::intermediate::IntermediateManager;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::location;

    #[tokio::test]
    async fn test_file_purge() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file-purge");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = FileId::random();
        let sst_dir = "table1";
        let path = location::sst_file_path(sst_dir, sst_file_id);

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let object_store = ObjectStore::new(builder).unwrap().finish();
        object_store.write(&path, vec![0; 4096]).await.unwrap();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let layer = Arc::new(AccessLayer::new(
            sst_dir,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer, None));

        {
            let handle = FileHandle::new(
                FileMeta {
                    region_id: 0.into(),
                    file_id: sst_file_id,
                    time_range: FileTimeRange::default(),
                    level: 0,
                    file_size: 4096,
                    available_indexes: Default::default(),
                    index_file_size: 0,
                    num_rows: 0,
                    num_row_groups: 0,
                },
                file_purger,
            );
            // mark file as deleted and drop the handle, we expect the file is deleted.
            handle.mark_deleted();
        }

        scheduler.stop(true).await.unwrap();

        assert!(!object_store.is_exist(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_file_purge_with_index() {
        common_telemetry::init_default_ut_logging();

        let dir = create_temp_dir("file-purge");
        let dir_path = dir.path().display().to_string();
        let builder = Fs::default().root(&dir_path);
        let sst_file_id = FileId::random();
        let sst_dir = "table1";

        let index_aux_path = dir.path().join("index_aux");
        let puffin_mgr = PuffinManagerFactory::new(&index_aux_path, 4096, None)
            .await
            .unwrap();
        let intm_mgr = IntermediateManager::init_fs(index_aux_path.to_str().unwrap())
            .await
            .unwrap();

        let path = location::sst_file_path(sst_dir, sst_file_id);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        object_store.write(&path, vec![0; 4096]).await.unwrap();

        let index_path = location::index_file_path(sst_dir, sst_file_id);
        object_store
            .write(&index_path, vec![0; 4096])
            .await
            .unwrap();

        let scheduler = Arc::new(LocalScheduler::new(3));
        let layer = Arc::new(AccessLayer::new(
            sst_dir,
            object_store.clone(),
            puffin_mgr,
            intm_mgr,
        ));

        let file_purger = Arc::new(LocalFilePurger::new(scheduler.clone(), layer, None));

        {
            let handle = FileHandle::new(
                FileMeta {
                    region_id: 0.into(),
                    file_id: sst_file_id,
                    time_range: FileTimeRange::default(),
                    level: 0,
                    file_size: 4096,
                    available_indexes: SmallVec::from_iter([IndexType::InvertedIndex]),
                    index_file_size: 4096,
                    num_rows: 1024,
                    num_row_groups: 1,
                },
                file_purger,
            );
            // mark file as deleted and drop the handle, we expect the sst file and the index file are deleted.
            handle.mark_deleted();
        }

        scheduler.stop(true).await.unwrap();

        assert!(!object_store.is_exist(&path).await.unwrap());
        assert!(!object_store.is_exist(&index_path).await.unwrap());
    }
}
